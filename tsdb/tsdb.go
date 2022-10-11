package tsdb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"time"
	"encoding/json"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

const (
	// The amount of overhead data per chunk. This assume 2 bytes to hold data length, 1 byte for version & 4 bytes for
	// CRC hash.
	chunkOverheadSize = 7

	// TSDB enforces that each segment must be at most 512MB.
	maxSegmentSize = 1024 * 1024 * 512

	// Keep chunks small for performance.
	maxChunkSize = 1024 * 16

	// TSDB allows a maximum of 120 samples per chunk.
	samplesPerChunk = 120

	// The size of the header for each segment file.
	segmentStartOffset = 8

	blockMetaTemplate = `{
	"version": 1,
	"ulid": "%s",
	"minTime": %d,
	"maxTime": %d,
	"stats": {
		"numSamples": %d,
		"numSeries": %d,
		"numChunks": %d
	},
	"compaction": {
		"level": 1,
		"sources": [
			"%s"
		]
	},
	"thanos": {
		"labels": {
			"replica": "42",
			"monitor": "test"
		},
		"downsample": {
			"resolution": 0
		}
	}
}`
)

type Duration struct {
    time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
    return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
    var v interface{}
    if err := json.Unmarshal(b, &v); err != nil {
        return err
    }
    switch value := v.(type) {
		case float64:
			d.Duration = time.Duration(value)
			return nil
		case string:
			var err error
			d.Duration, err = time.ParseDuration(value)
			if err != nil {
				return err
			}
			return nil
		default:
			return errors.New("invalid duration")
    }
}

type sequence struct {
	Time   time.Time `json:"time"`
	Value  float64   `json:"value"`
}

type Configuration struct {
	Metrics []struct {
		Name     string            `json:"name"`
		Labels   map[string]string `json:"labels"`
		Sequence []sequence        `json:"sequence"`
	} `json:"metrics"`
	OutputDir       string    `json:"output_dir"`        // The directory to place the generated TSDB blocks. Default /prometheus
	SampleInterval	Duration  `json:"sample_interval"` // How often to sample the metrics. Default 30s.
	BlockLength		Duration  `json:"block_length"`    // The length of time each block will cover. Default 2 hours.
	StartTime		time.Time `json:"start_time"`	  // Metrics will be produced from this time. Default 1 week.
	EndTime         time.Time `json:"end_time"`       // Metrics will be produced until this time. Default now.
}

type timeseries struct {
	ID                   uint64
	Name                 string
	Chunks               []chunks.Meta
	Labels               map[string]string
	Sequence             []sequence
	currentSequenceIndex int
	currentSequenceValue float64
}

func CreateThanosTSDB(config Configuration) error {
	if config.OutputDir == "" {
		config.OutputDir = "/prometheus"
	}

	now := time.Now()
	if config.StartTime.IsZero() {
		config.StartTime = now.Add(-time.Hour * 24 * 7)
	}

	if config.EndTime.IsZero() {
		config.EndTime = now
	}

	if config.StartTime.After(config.EndTime) {
		return errors.New("end time cannot come after start time")
	}

	zeroDuration := Duration{0}

	if config.SampleInterval == zeroDuration {
		config.SampleInterval = Duration{time.Second * 30}
	}

	if config.BlockLength == zeroDuration {
		config.BlockLength = Duration{time.Hour * 2}
	}

	rng := rand.New(rand.NewSource(now.UnixNano()))

	for blockStart := config.StartTime; blockStart.Before(config.EndTime); blockStart = blockStart.Add(config.BlockLength.Duration) {
		if err := createBlock(config, rng, blockStart, blockStart.Add(config.BlockLength.Duration)); err != nil {
			return err
		}
	}

	return nil
}

func createBlock(config Configuration, rng *rand.Rand, blockStart time.Time, blockEnd time.Time) error {
	// Generate block ID.
	blockULID, err := ulid.New(uint64(blockEnd.Unix()), rng)
	if err != nil {
		return errors.Wrap(err, "failed to create ULID for block")
	}
	outputDir := filepath.Join(config.OutputDir, blockULID.String())

	// Create sorted list of timeseries to write. These will not be populated with data yet.
	series := createEmptyTimeseries(config)

	// Store chunks in series & write them to disk.
	if err := populateChunks(series, outputDir, blockStart, blockEnd, config.SampleInterval.Duration); err != nil {
		return errors.Wrap(err, "failed to create chunks")
	}

	// Store references to these chunks in the index.
	if err := createIndex(series, outputDir, 0, len(config.Metrics)); err != nil {
		return errors.Wrap(err, "failed to create index")
	}

	// Add thanos metadata for this block.
	numChunks := int64(len(config.Metrics)) * (blockEnd.Sub(blockStart).Nanoseconds() / (config.SampleInterval.Duration * samplesPerChunk).Nanoseconds())
	thanosMeta := fmt.Sprintf(blockMetaTemplate, blockULID, blockStart.Unix()*1000, blockEnd.Unix()*1000, numChunks*samplesPerChunk, len(config.Metrics), numChunks, blockULID)
	if err := ioutil.WriteFile(filepath.Join(outputDir, "meta.json"), []byte(thanosMeta), 0755); err != nil {
		return errors.Wrap(err, "failed to write thanos metadata")
	}

	return nil
}

// createEmptyTimeseries will return `len(config.Metrics)` unique timeseries structs. Does not populate these timeseries with
// data yet.
func createEmptyTimeseries(config Configuration) []*timeseries {
	series := make([]*timeseries, len(config.Metrics))
	for i := 0; i < len(config.Metrics); i++ {
		series[i] = &timeseries{
			ID:   uint64(i),
			Name: config.Metrics[i].Name,
			Labels: config.Metrics[i].Labels,
			Sequence: config.Metrics[i].Sequence,
			currentSequenceIndex: 0,
			currentSequenceValue: 0,
		}
	}

	return series
}

// populateChunks will populate `series` with a list of chunks for each timeseries. The chunks will span the entire
// duration from blockStart to blockEnd. It will also write these chunks to the block's output directory.
func populateChunks(series []*timeseries, outputDir string, blockStart time.Time, blockEnd time.Time, sampleInterval time.Duration) error {
	cw, err := chunks.NewWriter(filepath.Join(outputDir, "chunks"))
	if err != nil {
		return err
	}

	// The reference into the chunk where a timeseries starts.
	ref := uint64(segmentStartOffset)
	seg := uint64(0)

	// The total size of the chunk.
	chunkLength := sampleInterval * samplesPerChunk

	// Populate each series with fake metrics.
	for _, serie := range series {
		// Segment block into small chunks.
		for chunkStart := blockStart; chunkStart.Before(blockEnd); chunkStart = chunkStart.Add(chunkLength) {
			ch := chunkenc.NewXORChunk()
			app, err := ch.Appender()
			if err != nil {
				return err
			}

			// Write series data for this chunk.
			for sampleTime := chunkStart; sampleTime.Before(chunkStart.Add(chunkLength)); sampleTime = sampleTime.Add(sampleInterval) {

				sampleInSequenceFound := false
				for index, sampleInSequence := range serie.Sequence[serie.currentSequenceIndex:] {
					if (sampleInSequence.Time.After(sampleTime) || sampleInSequence.Time.Equal(sampleTime)) && (sampleInSequence.Time.Sub(sampleTime).Abs() <= sampleInterval) {
						serie.currentSequenceValue = sampleInSequence.Value
						app.Append(sampleInSequence.Time.Unix()*1000, serie.currentSequenceValue)
						serie.currentSequenceIndex = index + 1
						sampleInSequenceFound= true
						break
					}
				}

				if !sampleInSequenceFound {
					app.Append(sampleTime.Unix()*1000, serie.currentSequenceValue)
				}
			}

			// Calcuate size of this chunk. This is the amount of bytes written plus the chunk overhead. See
			// https://github.com/prometheus/tsdb/blob/master/docs/format/chunks.md for a breakdown of the overhead.
			// Assumes that the len uvarint has size 2.
			size := uint64(len(ch.Bytes())) + chunkOverheadSize
			if size > maxChunkSize {
				return errors.Errorf("chunk too big, calculated size %d > %d", size, maxChunkSize)
			}

			// Reference a new segment if the current is out of space.
			if ref+size > maxSegmentSize {
				ref = segmentStartOffset
				seg++
			}

			chunkStartMs := chunkStart.Unix() * 1000
			cm := chunks.Meta{
				Chunk:   ch,
				MinTime: chunkStartMs,
				MaxTime: chunkStartMs + chunkLength.Nanoseconds()/(1000*1000),
				Ref:     ref | (seg << 32),
			}

			serie.Chunks = append(serie.Chunks, cm)

			ref += size
		}

		if err := cw.WriteChunks(serie.Chunks...); err != nil {
			return err
		}
	}

	if err := cw.Close(); err != nil {
		return err
	}

	return nil
}

// createIndex will write the index file. It should reference the chunks previously created.
func createIndex(series []*timeseries, outputDir string, seriesStartIndex int, totalSeries int) error {
	//values := make([]string, len(series))
	seriesNames := make([]string, len(series))
	labelset := make([]labels.Labels, len(series))
	iw, err := index.NewWriter(filepath.Join(outputDir, "index"))
	if err != nil {
		return err
	}

	// Populate metric name label for metric
	for i, serie := range series {
		seriesNames[i] = serie.Name
	}

	symbols := map[string]struct{}{
		"": {},
		"__name__": {},
		"instance": {},
		"job": {},
	}

	for _, serie := range series {
		symbols[serie.Name] = struct{}{}
		for key, value := range serie.Labels {
			symbols[key] = struct{}{}
			symbols[value] = struct{}{}
		}
	}

	// Add the symbol table from all symbols we use.
	if err := iw.AddSymbols(symbols); err != nil {
		return err
	}

	// Add chunk references.
	for i, serie := range series {
		labelset[i] = labels.Labels{
			{Name: "__name__", Value: serie.Name},
		}

		for key, value := range serie.Labels {
			labelset[i] = append(labelset[i], labels.Label{Name: key, Value: value})
		}

		if err := iw.AddSeries(serie.ID, labelset[i], serie.Chunks...); err != nil {
			return errors.Wrapf(err, "failed to write timeseries for %s", serie.Name)
		}
	}

	// Create & populate postings.
	postings := index.NewMemPostings()
	for i, serie := range series {
		postings.Add(serie.ID, labelset[i])
	}

	// Add references to index for each label name/value pair.
	for _, l := range postings.SortedKeys() {
		if err := iw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}

	// Output index to file.
	if err := iw.Close(); err != nil {
		return err
	}

	return nil
}
