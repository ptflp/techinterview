package main

// Minimal PostgreSQL heap page inspector for 8KiB pages.
// Focus: page header, ItemIdData, HeapTupleHeader, and sample attr decode
// (id BIGINT, name TEXT/varlena short/long). No indexes, no FSM/VM.
// Tested against layouts similar to PG12 on little-endian.
//
// NOTE: This is a learning tool; it does not handle TOAST pointers,
// compressed varlena, or all visibility/infomask combinations.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

const (
	PageSize          = 8192
	PageHeaderByteLen = 24
	ItemIDByteLen     = 4
)

// -------- Page header (bufpage.h) --------

type PageHeader struct {
	XLogID            uint32 // pd_lsn.xlogid
	XRecOff           uint32 // pd_lsn.xrecoff
	PdChecksum        uint16
	PdFlags           uint16
	PdLower           uint16 // start of line pointers area end
	PdUpper           uint16 // start of tuples (from the end)
	PdSpecial         uint16 // start of special space (heap: == BLCKSZ)
	PdPagesizeVersion uint16
	PdPruneXID        uint32
}

func readPageHeader(r io.Reader) (*PageHeader, error) {
	h := &PageHeader{}
	if err := binary.Read(r, binary.LittleEndian, h); err != nil {
		return nil, err
	}
	return h, nil
}

// -------- ItemIdData (itemid.h) --------
//
// On-disk: two uint16. 2 flag bits are split across them:
//  - high bit of lp_off (bit15) is low flag bit
//  - low bit of lp_len (bit0) is high flag bit
//
// Decoding to fields:
//  off15  = lp_off & 0x7FFF
//  len15  = lp_len >> 1
//  flags2 = ((lp_off>>15)&0x01) | ((lp_len<<1)&0x02)

type rawItemID struct {
	LpOff uint16
	LpLen uint16
}

type ItemID struct {
	LpOff uint16 // 15-bit offset from page start
	LpLen uint16 // 15-bit length
	Flags byte   // 2-bit flags
	Index int    // index within line pointer array
}

const (
	LP_UNUSED   = 0
	LP_NORMAL   = 1
	LP_REDIRECT = 2
	LP_DEAD     = 3
)

func readItemIDs(r io.Reader, header *PageHeader) ([]ItemID, error) {
	n := int(header.PdLower-PageHeaderByteLen) / ItemIDByteLen
	if n < 0 || n > (PageSize-PageHeaderByteLen)/ItemIDByteLen {
		return nil, fmt.Errorf("bad PdLower=%d; computed itemId count=%d", header.PdLower, n)
	}
	out := make([]ItemID, 0, n)
	for i := 0; i < n; i++ {
		var raw rawItemID
		if err := binary.Read(r, binary.LittleEndian, &raw); err != nil {
			return nil, fmt.Errorf("read ItemIdData[%d]: %w", i, err)
		}
		flags := byte(((raw.LpOff >> 15) & 0x01) | ((raw.LpLen << 1) & 0x02))
		item := ItemID{
			LpOff: raw.LpOff & 0x7FFF,
			LpLen: raw.LpLen >> 1,
			Flags: flags,
			Index: i + 1, // 1-based, like offset numbers
		}
		out = append(out, item)
	}
	return out, nil
}

// -------- Heap tuple header (htup_details.h) --------
//
// Minimal subset; sizes match common PG builds (little-endian).
// This maps to: t_xmin,t_xmax,t_cid/t_xvac, t_ctid(blockhi,blocklo,offset),
// t_infomask2, t_infomask, t_hoff.

type RowHeader struct {
	Xmin uint32
	Xmax uint32
	CId  uint32
	// ItemPointerData (ctid)
	CTIDBlockHi uint16
	CTIDBlockLo uint16
	CTIDOffset  uint16
	InfoMask2   uint16 // low 11 bits = natts
	InfoMask    uint16
	Hoff        byte
}

func (rh *RowHeader) Natts() int { return int(rh.InfoMask2 & 0x07FF) }

// t_infomask flags we care about (subset)
const (
	HEAP_HASNULL        = 0x0001
	HEAP_HASVARWIDTH    = 0x0002
	HEAP_HASEXTERNAL    = 0x0008 // TOAST pointer
	HEAP_MOVED_OFF      = 0x0010
	HEAP_MOVED_IN       = 0x0020
	HEAP_XMAX_INVALID   = 0x0100
	HEAP_XMAX_COMMITTED = 0x0200
)

// Align helpers per attalign: 'c'=1, 's'=2, 'i'=4, 'd'=8
func align(off int, align byte) int {
	var a int
	switch align {
	case 'c':
		a = 1
	case 's':
		a = 2
	case 'i':
		a = 4
	case 'd':
		a = 8
	default:
		a = 1
	}
	m := (off + (a - 1)) & ^(a - 1)
	return m
}

// Varlenas (postgres.h): detect 1-byte vs 4-byte header on little-endian.
// Returns payload slice and new offset.
// This simplified reader supports:
// - 1-byte short varlena (xxxxxxx1) up to 126 bytes
// - 4-byte uncompressed (.... ..00) (length includes the 4 bytes)
// Does NOT support compressed or TOAST pointer (you'll get an error).
func readVarlenaLE(buf []byte, off int) (payload []byte, next int, err error) {
	if off >= len(buf) {
		return nil, off, io.ErrUnexpectedEOF
	}
	first := buf[off]
	if first&0x01 == 1 {
		// short varlena: length in upper 7 bits + includes itself
		l := int(first >> 1) // length including the 1-byte header
		if l < 1 {
			return nil, off, errors.New("short varlena length < 1")
		}
		total := l
		if off+total > len(buf) {
			return nil, off, io.ErrUnexpectedEOF
		}
		return buf[off+1 : off+total], off + total, nil
	}
	// Check 4-byte header (xxxxxx00 or xxxxxx10)
	if off+4 > len(buf) {
		return nil, off, io.ErrUnexpectedEOF
	}
	h := binary.LittleEndian.Uint32(buf[off : off+4])
	// lowest two bits are flags; if ==00 -> uncompressed aligned
	switch h & 0x03 {
	case 0x00: // uncompressed 4-byte len
		length := int(h >> 2) // length including the 4 bytes
		if length < 4 {
			return nil, off, errors.New("invalid long varlena length")
		}
		total := length
		if off+total > len(buf) {
			return nil, off, io.ErrUnexpectedEOF
		}
		return buf[off+4 : off+total], off + total, nil
	case 0x10, 0x02: // compressed (xxxxxx10) -> not handled here
		return nil, off, errors.New("compressed varlena not supported")
	case 0x01: // TOAST pointer (00000001) -> not supported
		return nil, off, errors.New("TOAST pointer varlena not supported")
	default:
		return nil, off, errors.New("unknown varlena header pattern")
	}
}

// Decode two attributes of the demo table:
// 1) id BIGINT (attlen=8, attalign='d')
// 2) name TEXT  (varlena, attalign='i')
type DemoRow struct {
	ID   int64
	Name string
}

func decodeDemoRow(buf []byte, rh *RowHeader) (DemoRow, error) {
	var out DemoRow

	// Start of DATA area
	if int(rh.Hoff) > len(buf) {
		return out, io.ErrUnexpectedEOF
	}
	off := int(rh.Hoff)

	// NULL bitmap if present
	hasNulls := (rh.InfoMask & HEAP_HASNULL) != 0
	var nullmap []byte
	if hasNulls {
		// ceil(natts/8)
		nb := (rh.Natts() + 7) / 8
		if off+nb > len(buf) {
			return out, io.ErrUnexpectedEOF
		}
		nullmap = buf[off : off+nb]
		off += nb
	}
	isNull := func(attIdx int) bool {
		if !hasNulls {
			return false
		}
		byteIdx := attIdx / 8
		bit := byte(1 << (attIdx % 8))
		return (nullmap[byteIdx] & bit) != 0
	}

	// ---- attr 1: id BIGINT ----
	off = align(off, 'd')
	if !isNull(0) {
		if off+8 > len(buf) {
			return out, io.ErrUnexpectedEOF
		}
		out.ID = int64(binary.LittleEndian.Uint64(buf[off : off+8]))
		off += 8
	}

	// ---- attr 2: name TEXT (varlena) ----
	off = align(off, 'i')
	if !isNull(1) {
		payload, next, err := readVarlenaLE(buf, off)
		if err != nil {
			return out, fmt.Errorf("read text varlena: %w", err)
		}
		out.Name = string(payload) // assuming UTF-8 and no compression/TOAST
		off = next
	}

	return out, nil
}

// Utility to dump one page (8KiB) from a relation file at given page index.
func dumpPage(filePath string, pageNo int, decodeDemo bool) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to page
	off := int64(pageNo) * PageSize
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}

	page := make([]byte, PageSize)
	n, err := io.ReadFull(f, page)
	if err != nil {
		return fmt.Errorf("read page: %w", err)
	}
	if n != PageSize {
		return fmt.Errorf("short read: got %d", n)
	}

	r := bytes.NewReader(page)
	hdr, err := readPageHeader(r)
	if err != nil {
		return err
	}

	fmt.Printf("== Page %d ==\n", pageNo)
	fmt.Printf("pd_lower=%d pd_upper=%d pd_special=%d  | free=%d bytes\n",
		hdr.PdLower, hdr.PdUpper, hdr.PdSpecial, int(hdr.PdUpper)-int(hdr.PdLower))
	fmt.Printf("lsn=(%d,%d) checksum=%d flags=0x%04x pagesize_ver=%d prune_xid=%d\n",
		hdr.XLogID, hdr.XRecOff, hdr.PdChecksum, hdr.PdFlags, hdr.PdPagesizeVersion, hdr.PdPruneXID)

	itemIDs, err := readItemIDs(r, hdr)
	if err != nil {
		return err
	}
	fmt.Printf("line pointers: %d\n", len(itemIDs))

	for _, it := range itemIDs {
		fmt.Printf(" [%2d] lp_off=%4d lp_len=%3d flags=%d", it.Index, it.LpOff, it.LpLen, it.Flags)
		switch it.Flags {
		case LP_UNUSED:
			fmt.Printf(" (UNUSED)\n")
			continue
		case LP_REDIRECT:
			fmt.Printf(" (REDIRECT)\n")
			continue
		case LP_DEAD:
			fmt.Printf(" (DEAD)\n")
			// continue to show header anyway? Skip here:
			fmt.Printf("\n")
			continue
		default:
			fmt.Printf(" (NORMAL)\n")
		}

		// Bounds check
		start := int(it.LpOff)
		end := start + int(it.LpLen)
		if start < 0 || end > len(page) || start >= end {
			fmt.Printf("      ERROR: tuple span out of page bounds\n")
			continue
		}

		tuple := page[start:end]
		rr := bytes.NewReader(tuple)
		var rh RowHeader
		if err := binary.Read(rr, binary.LittleEndian, &rh); err != nil {
			fmt.Printf("      ERROR: read row header: %v\n", err)
			continue
		}

		fmt.Printf("      xmin=%d xmax=%d ctid=(%d,%d) natts=%d hoff=%d infomask=0x%04x infomask2=0x%04x\n",
			rh.Xmin, rh.Xmax,
			int(rh.CTIDBlockHi)<<16|int(rh.CTIDBlockLo), rh.CTIDOffset,
			rh.Natts(), rh.Hoff, rh.InfoMask, rh.InfoMask2)

		if decodeDemo {
			row, err := decodeDemoRow(tuple, &rh)
			if err != nil {
				fmt.Printf("      decode demo row: %v\n", err)
			} else {
				fmt.Printf("      demo: id=%d, name=%q\n", row.ID, row.Name)
			}
		}
	}

	return nil
}

func main() {
	var path string
	var page int
	var demo bool
	flag.StringVar(&path, "file", "", "Path to relation file (e.g. base/DBOID/RELOID)")
	flag.IntVar(&page, "page", 0, "Page number (0-based)")
	flag.BoolVar(&demo, "demo", true, "Decode demo columns (id BIGINT, name TEXT)")
	flag.Parse()

	path = "/run/media/deck/steamdrive/go/src/github.com/ptflp/techinterview/2.db/57344"
	if path == "" {
		fmt.Println("Usage:")
		fmt.Println("  pgheapdump -file /path/to/16567 -page 0 [-demo=true]")
		os.Exit(2)
	}

	if err := dumpPage(path, page, demo); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
