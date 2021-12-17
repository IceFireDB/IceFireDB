/*
 * @Author: gitsrc
 * @Date: 2021-12-17 17:52:52
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-12-17 17:55:43
 * @FilePath: /IceFireDB/driver/ipfs/iterator.go
 */
package ipfs

import (
	"io/ioutil"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type Iterator struct {
	it     iterator.Iterator
	rShell *shell.Shell
}

func (it *Iterator) Key() []byte {
	return it.it.Key()
}

func (it *Iterator) Value1() []byte {
	return it.it.Value()
}

func (it *Iterator) Value() []byte {

	v := it.it.Value()
	reader, err := it.rShell.Cat(string(v))
	if err != nil {
		return nil
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil
	}

	return data
}

func (it *Iterator) Close() error {
	if it.it != nil {
		it.it.Release()
		it.it = nil
	}
	return nil
}

func (it *Iterator) Valid() bool {
	return it.it.Valid()
}

func (it *Iterator) Next() {
	it.it.Next()
}

func (it *Iterator) Prev() {
	it.it.Prev()
}

func (it *Iterator) First() {
	it.it.First()
}

func (it *Iterator) Last() {
	it.it.Last()
}

func (it *Iterator) Seek(key []byte) {
	it.it.Seek(key)
}
