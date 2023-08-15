package multibuffer

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type MultibufferProductScan struct {
	tx         *tx.Transaction
	lhsScan    query.Scan
	rhsScan    query.Scan
	prodScan   query.Scan
	filename   string
	layout     *record.Layout
	chunkSize  int
	nextBlkNum int
	fileSize   int
}

func NewMultibufferProductScan(tx *tx.Transaction, lhsScan query.Scan, tblName string, layout *record.Layout) *MultibufferProductScan {
	bufferNeeds := &BufferNeeds{}
	return &MultibufferProductScan{
		tx:       tx,
		lhsScan:  lhsScan,
		filename: tblName + ".tbl",
		layout:   layout,
		fileSize: tx.Size(tblName + ".tbl"),
		chunkSize: bufferNeeds.BestFactor(
			tx.AvailableBuffs(),
			tx.Size(tblName+".tbl"),
		),
	}
}

func (mps *MultibufferProductScan) BeforeFirst() {
	mps.nextBlkNum = 0
	mps.useNextChunk()
}

func (mps *MultibufferProductScan) Next() bool {
	for !mps.prodScan.Next() {
		if !mps.useNextChunk() {
			return false
		}
	}
	return true
}

func (mps *MultibufferProductScan) Close() {
	mps.prodScan.Close()
}

func (mps *MultibufferProductScan) GetVal(fldName string) *record.Constant {
	return mps.prodScan.GetVal(fldName)
}

func (mps *MultibufferProductScan) GetInt(fldName string) int {
	return mps.prodScan.GetInt(fldName)
}

func (mps *MultibufferProductScan) GetString(fldName string) string {
	return mps.prodScan.GetString(fldName)
}

func (mps *MultibufferProductScan) HasField(fldName string) bool {
	return mps.prodScan.HasField(fldName)
}

func (mps *MultibufferProductScan) useNextChunk() bool {
	if mps.nextBlkNum >= mps.fileSize {
		return false
	}
	if mps.rhsScan != nil {
		mps.rhsScan.Close()
	}
	end := mps.nextBlkNum + mps.chunkSize - 1
	if end >= mps.fileSize {
		end = mps.fileSize - 1
	}
	mps.rhsScan = NewChunkScan(mps.tx, mps.filename, mps.layout, mps.nextBlkNum, end)
	mps.lhsScan.BeforeFirst()
	mps.prodScan = query.NewProductScan(mps.lhsScan, mps.rhsScan)
	mps.nextBlkNum = end + 1
	return true
}
