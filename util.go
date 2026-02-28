package wi

import (
	"bytes"
	"encoding/json"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

func RemoveFile(filePath string) {
	// 使用os.Stat检查文件是否存在
	_, err := os.Stat(filePath)
	if err == nil {
		// 文件存在，尝试删除
		err := os.Remove(filePath)
		if err != nil {
			log.Printf("Error removing file: %s\n", err)
		}
	}
}

func removeDuplicates(data []Data) []Data {
	seen := make(map[uint32]bool)
	result := make([]Data, 0, len(data))
	for _, d := range data {
		if !seen[d.Block] {
			seen[d.Block] = true
			result = append(result, d)
		}
	}
	return result
}

func Mkdir(dir string) {
	if len(dir) == 0 {
		return
	}
	// 使用 MkdirAll 确保目录存在
	err := os.MkdirAll(dir, 0770)
	if err != nil {
		log.Printf("Error creating directory: %v", err)
	}
}

func writeString(b *bytes.Buffer, str string) error {
	_, err := b.WriteString(str) // write str
	if err != nil {
		return err
	}

	err = b.WriteByte(0) // write 0 byte
	if err != nil {
		return err
	}
	return nil
}

func readString(r *bytes.Buffer) (string, error) {
	str, err := r.ReadString(0) //read filename
	if err != nil {
		return "", err
	}

	str = strings.TrimRight(str, "\x00") // remove the 0-byte
	if len(str) == 0 {
		return "", err
	}
	return str, nil
}

func Hash(ptr unsafe.Pointer) uint32 {
	return uint32(uintptr(ptr))
}

// block sequence [1 2 3 5], return 3
func findConsecutive(data []Data) int {
	block := data[0].Block
	var i = len(data)
	for k, d := range data {
		if d.Block != block {
			i = k
			break
		}
		block++
	}
	return i
}

func write(filePath string, data []Data) []Data {
	if len(data) == 0 {
		return nil
	}
	data = removeDuplicates(data)
	sort.Slice(data, func(i, j int) bool {
		return data[i].Block < data[j].Block
	})
	i := findConsecutive(data)

	dir := filepath.Dir(filePath)
	Mkdir(dir)
	writeTo(filePath, data[:i])
	if i < len(data) {
		// return leftover
		return data[i:]
	}
	return nil
}

func EncodeGif(w io.Writer, filename string, gifImg *gif.GIF) {
	err := gif.EncodeAll(w, gifImg)
	if err != nil {
		log.Printf("encode file failed, %v", err)
	} else {
		log.Printf("%s saved", filename)
	}
}

func EncodeImg(w io.Writer, filename string, img image.Image) error {
	ext := filepath.Ext(filename)
	switch ext {
	case ".png", ".webp":
		err := png.Encode(w, img)
		if err != nil {
			return err
		}
	case ".jpg", ".jpeg":
		err := jpeg.Encode(w, img, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetHigh16(num uint32) uint16 {
	return uint16(num >> 16)
}

func GetLow16(num uint32) uint16 {
	return uint16(num & 0xFFFF)
}

func SplitUint32(num uint32) (high, low uint16) {
	high = uint16(num >> 16)
	low = uint16(num & 0xFFFF)
	return high, low
}

func SetHigh16(num uint32, high uint16) uint32 {
	return (num & 0x0000FFFF) | (uint32(high) << 16)
}

func SetLow16(num uint32, low uint16) uint32 {
	return (num & 0xFFFF0000) | uint32(low)
}

func CombineUint32(high, low uint16) uint32 {
	return uint32(high)<<16 | uint32(low)
}

type RangeTracker struct {
	latestBlock uint32
	ranges      []Range
}

func (r *RangeTracker) GetProgress() int {
	cnt := uint32(0)
	for _, rng := range r.ranges {
		cnt += rng.end - rng.start + 1
	}
	return 100 - int(float32(cnt)/float32(r.latestBlock-1)*100)
}

func (r *RangeTracker) Set(ranges []Range) {
	n := len(ranges)
	if n > 0 {
		r.latestBlock = ranges[n-1].end + 1
	}
	r.ranges = ranges
}

func (r *RangeTracker) Merge(x RangeTracker) {
	x.Exclude(r.ranges)
	for _, rng := range x.ranges {
		r.add(rng)
	}
}

func (r *RangeTracker) Exclude(ranges []Range) {
	for _, rng := range ranges {
		r.remove(rng)
	}
}

func (r *RangeTracker) Add(rg Range) {
	if rg.start < r.nextBlock() {
		// 考虑补帧
		r.remove(rg)
		if rg.end < r.nextBlock() {
			return
		}
	} else if rg.start > r.nextBlock() {
		// 考虑丢帧
		r.add(Range{r.nextBlock(), rg.start - 1})
	}
	r.latestBlock = rg.end
}

// Contains return true if rg is not missing.
func (r *RangeTracker) Contains(rg Range) bool {
	if rg.end >= r.nextBlock() {
		return false
	}
	if len(r.ranges) == 0 {
		return true
	}
	for _, rng := range r.ranges {
		if rng.overlaps(rg) {
			return false
		}
	}
	return true
}

func (r *RangeTracker) GetRanges() []Range {
	ret := make([]Range, len(r.ranges))
	copy(ret, r.ranges)
	return ret
}

func (r *RangeTracker) remove(rg Range) {
	ret := make([]Range, 0, len(r.ranges))
	for _, v := range r.ranges {
		if rg.before(v) || rg.after(v) {
			ret = append(ret, v)
		} else if rg.contains(v) {
			// no op, v removed
		} else {
			if rg.startWithin(v) {
				ret = append(ret, Range{v.start, rg.start - 1})
			}
			if rg.endWithin(v) {
				ret = append(ret, Range{rg.end + 1, v.end})
			}
		}
	}
	r.ranges = ret
}

func (r *RangeTracker) add(rg Range) {
	r.ranges = append(r.ranges, rg)
}

func (r *RangeTracker) isCompleted() bool {
	return len(r.ranges) == 0
}

func (r *RangeTracker) nextBlock() uint32 {
	return r.latestBlock + 1
}

type Packet struct {
	Block uint32
	Data  []byte
}

type CircularBuffer struct {
	data     []Packet
	size     int
	capacity int
	head     int
	tail     int
	mu       sync.RWMutex
}

func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		data:     make([]Packet, capacity),
		capacity: capacity,
	}
}

func (cb *CircularBuffer) Write(packet Packet) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.writeNext(packet)

	if cb.size < cb.capacity {
		cb.size++
	} else {
		cb.readNext()
	}
}

func (cb *CircularBuffer) writeNext(packet Packet) {
	cb.data[cb.head] = packet
	cb.head = (cb.head + 1) % cb.capacity
}

func (cb *CircularBuffer) readNext() {
	cb.tail = (cb.tail + 1) % cb.capacity
}

func (cb *CircularBuffer) Read(ranges []Range) []Packet {
	ret := make([]Packet, 0, len(ranges)*2)
	for i := 0; i < cb.size; i++ {
		idx := (cb.tail + i) % cb.capacity
		for _, r := range ranges {
			if r.Within(cb.data[idx].Block) {
				ret = append(ret, cb.data[idx])
			}
		}
	}
	return ret
}

func (cb *CircularBuffer) Size() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.size
}

func (cb *CircularBuffer) Clear() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.head = 0
	cb.tail = 0
	cb.size = 0
	cb.data = cb.data[:0]
}

func Load(filePath string) *Client {
	log.Printf("load config from file: %s", filePath)
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return nil
	}
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("[%s] open file failed: %v", filePath, err)
		return nil
	}
	defer file.Close()
	var c Client
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&c)
	if err != nil {
		log.Printf("[%s] decode failed: %v", filePath, err)
		return nil
	}
	return &c
}

func (c *Client) Store() {
	dir := c.DataDir + "/"
	Mkdir(dir)
	filePath := dir + c.ConfigName
	log.Printf("store config to file: %s", filePath)
	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("[%s] create file failed: %v", filePath, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(&c)
	if err != nil {
		log.Printf("[%s] encode file failed: %v", filePath, err)
	}
}
