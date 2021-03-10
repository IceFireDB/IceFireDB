package main

// func cmdPDEL(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) != 2 {
// 		return nil, uhaha.ErrWrongNumArgs
// 	}
// 	pattern := args[1]
// 	min, max := match.Allowable(pattern)
// 	var keys []string
// 	iter := db.NewIterator(nil, nil)
// 	for ok := iter.Seek([]byte(min)); ok; ok = iter.Next() {
// 		key := string(iter.Key())
// 		if pattern != "*" {
// 			if key >= max {
// 				break
// 			}
// 			if !match.Match(key, pattern) {
// 				continue
// 			}
// 		}
// 		keys = append(keys, key)
// 	}
// 	iter.Release()
// 	err := iter.Error()
// 	if err != nil {
// 		return nil, err
// 	}
// 	var batch leveldb.Batch
// 	for _, key := range keys {
// 		batch.Delete([]byte(key))
// 	}
// 	if err := db.Write(&batch, nil); err != nil {
// 		return nil, err
// 	}
// 	return redcon.SimpleString("OK"), nil
// }

// func cmdKEYS(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) < 2 {
// 		return nil, uhaha.ErrWrongNumArgs
// 	}
// 	var withvalues bool
// 	var pivot string
// 	var usingPivot bool
// 	var desc bool
// 	var excl bool
// 	limit := math.MaxUint32
// 	for i := 2; i < len(args); i++ {
// 		switch strings.ToLower(args[i]) {
// 		default:
// 			return nil, uhaha.ErrSyntax
// 		case "withvalues":
// 			withvalues = true
// 		case "excl":
// 			excl = true
// 		case "desc":
// 			desc = true
// 		case "pivot":
// 			i++
// 			if i == len(args) {
// 				return nil, uhaha.ErrSyntax
// 			}
// 			pivot = args[i]
// 			usingPivot = true
// 		case "limit":
// 			i++
// 			if i == len(args) {
// 				return nil, uhaha.ErrSyntax
// 			}
// 			n, err := strconv.ParseInt(args[i], 10, 64)
// 			if err != nil || n < 0 {
// 				return nil, uhaha.ErrSyntax
// 			}
// 			limit = int(n)
// 		}
// 	}
// 	var min, max string

// 	pattern := args[1]
// 	var all bool
// 	if pattern == "*" {
// 		all = true
// 	} else {
// 		min, max = match.Allowable(pattern)
// 	}
// 	var ok bool
// 	var keys []string
// 	var values []string
// 	iter := db.NewIterator(nil, nil)
// 	step := func() bool {
// 		if desc {
// 			return iter.Prev()
// 		}
// 		return iter.Next()
// 	}
// 	if usingPivot {
// 		ok = iter.Seek([]byte(pivot))
// 		if ok && excl {
// 			key := string(iter.Key())
// 			if key == pivot {
// 				ok = step()
// 			}
// 		}
// 	} else {
// 		if all {
// 			if desc {
// 				ok = iter.Last()
// 			} else {
// 				ok = iter.First()
// 			}
// 		} else {
// 			if desc {
// 				ok = iter.Seek([]byte(max))
// 			} else {
// 				ok = iter.Seek([]byte(min))
// 			}
// 		}
// 	}
// 	for ; ok; ok = step() {
// 		if len(keys) == limit {
// 			break
// 		}
// 		key := string(iter.Key())
// 		if !all {
// 			if desc {
// 				if key < min {
// 					break
// 				}
// 			} else {
// 				if key > max {
// 					break
// 				}
// 			}
// 			if !match.Match(key, pattern) {
// 				continue
// 			}
// 		}
// 		keys = append(keys, key)
// 		if withvalues {
// 			values = append(values, string(iter.Value()))
// 		}
// 	}
// 	iter.Release()
// 	err := iter.Error()
// 	if err != nil {
// 		return nil, err
// 	}
// 	var res []string
// 	if withvalues {
// 		for i := 0; i < len(keys); i++ {
// 			res = append(res, keys[i], values[i])
// 		}
// 	} else {
// 		for i := 0; i < len(keys); i++ {
// 			res = append(res, keys[i])
// 		}
// 	}
// 	return res, nil
// }
