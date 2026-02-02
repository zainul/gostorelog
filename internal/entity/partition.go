package entity

// Partition represents a partition containing multiple segments
type Partition struct {
	Key           string     `json:"key"`
	Segments      []*Segment `json:"segments"`
	CurrentOffset uint64     `json:"current_offset"`
	DataDir       string     `json:"data_dir"`
	MaxFileSize   uint64     `json:"max_file_size"`
}

// NewPartition creates a new partition
func NewPartition(key string, dataDir string, maxFileSize uint64) *Partition {
	p := &Partition{
		Key:         key,
		Segments:    []*Segment{},
		DataDir:     dataDir,
		MaxFileSize: maxFileSize,
	}
	// Create initial segment
	p.createNewSegment()
	return p
}

// createNewSegment creates a new segment for the partition
func (p *Partition) createNewSegment() {
	baseOffset := p.CurrentOffset
	segment := NewSegment(p.Key, baseOffset, p.MaxFileSize, p.DataDir)
	p.Segments = append(p.Segments, segment)
}

// GetActiveSegment returns the active segment for writing
func (p *Partition) GetActiveSegment() *Segment {
	if len(p.Segments) == 0 {
		p.createNewSegment()
	}
	active := p.Segments[len(p.Segments)-1]
	if !active.IsActive {
		p.createNewSegment()
		active = p.Segments[len(p.Segments)-1]
	}
	return active
}

// AppendRecord appends a record to the partition
func (p *Partition) AppendRecord(record *Record, recordSize uint64) error {
	active := p.GetActiveSegment()
	if active.ShouldRollOver(recordSize) {
		active.IsActive = false
		p.createNewSegment()
		active = p.GetActiveSegment()
	}
	record.Offset = active.NextOffset
	active.AddRecord(recordSize)
	p.CurrentOffset = active.NextOffset
	return nil
}