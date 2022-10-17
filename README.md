# Microsoft Visual FoxPro DBF for Go

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/Valentin-Kaiser/go-dbase)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://github.com/Valentin-Kaiser/go-dbase/blob/main/LICENSE)
[![Linters](https://github.com/Valentin-Kaiser/go-dbase/workflows/Linters/badge.svg)](https://github.com/Valentin-Kaiser/go-dbase)
[![CodeQL](https://github.com/Valentin-Kaiser/go-dbase/workflows/CodeQL/badge.svg)](https://github.com/Valentin-Kaiser/go-dbase)
[![Examples](https://github.com/Valentin-Kaiser/go-dbase/workflows/Examples/badge.svg)](https://github.com/Valentin-Kaiser/go-dbase)
[![Go Report](https://goreportcard.com/badge/github.com/Valentin-Kaiser/go-dbase)](https://goreportcard.com/report/github.com/Valentin-Kaiser/go-dbase)

**Golang package for reading and writing FoxPro dBase table and memo files.**

## Features 

There are several similar packages but they are not suited for our use case, this package implements the following features:

| Feature | [go-dbase](https://github.com/Valentin-Kaiser/go-dbase) | [go-dbf](https://github.com/LindsayBradford/go-dbf) | [go-foxpro-dbf](https://github.com/SebastiaanKlippert/go-foxpro-dbf) | 
| --- | --- | --- | --- |
| Encoding support ¹ | ✅ | ✅[*](https://github.com/LindsayBradford/go-dbf/issues/3) | ✅ |
| Read | ✅ | ✅ | ✅ |
| Write | ✅  | ✅ | ❌ |
| FPT (memo) file support | ✅ | ❌ | ✅ |
| Struct, json, map conversion | ✅ | ❌ | ✅ |
| IO efficiency ² | ✅ | ❌ | ✅ |
| Full data type support | ✅ | ❌ | ❌ |
| Exclusive Read/Write³ | ✅ | ❌ | ❌ |
| Search  | ✅ | ❌ | ❌ |
| Create new tables, including schema | ✅ | ❌ | ❌ |
| Open database | ✅ | ❌ | ❌ |

> ¹ This package currently supports 13 of the 25 possible encodings, but a universal encoder will be provided for other code pages that can be extended at will. A list of supported encodings can be found [here](#supported-encodings). The conversion in the go-foxpro-dbf package is extensible, but only Windows-1250 as default and the code page is not interpreted. 

> ² IO efficiency is achieved by using one file handle for the DBF file and one file handle for the FPT file. This allows for non blocking IO and the ability to read files while other processes are accessing these. In addition, only the required positions in the file are read instead of keeping a copy of the entire file in memory.

> ³ The files can be opened completely exclusively and when writing a file, the data block to be written can be locked during the process. This is done to prevent other processes from writing the same data block. When reading, this is not a concern as the data is not changed.

> **Disclaimer:** _This library should never be used to develop new software solutions with dbase tables. The creation of new tables only serves to transfer old databases or to remove faulty data._

### Supported column types

At this moment not all FoxPro column types are supported. 
When reading column values, the value returned by this package is always `interface{}`. 
If you need to cast this to the correct value, helper functions are provided.

The supported column types with their return Go types are: 

| Column Type | Column Type Name | Golang type |
|------------|-----------------|-------------|
| C | Character | string |
| Y | Currency | float64 |
| B | Double | float64 |
| D | Date | time.Time |
| T | DateTime | time.Time | 	
| F | Float | float64 |
| I | Integer | int32 |
| L | Logical | bool |
| M | Memo  | string |
| M | Memo (Binary) | []byte |
| N | Numeric (0 decimals) | int64 |
| N | Numeric (with decimals) | float64 |
| Q | Varbinary | []byte |
| V | Varchar | []byte |
| W | Blob | []byte |
| G | General | []byte |
| P | Picture | []byte |


> If you need more information about dbase data types take a look here: [Microsoft Visual Studio Foxpro](https://learn.microsoft.com/en-us/previous-versions/visualstudio/foxpro/74zkxe2k(v=vs.80))

### Supported encodings

The following encodings are supported by this package:

| Code page | Platform | Code page identifier |
| --- | --- | --- |
| 437 | U.S. MS-DOS | x01 |
| 850 | International MS-DOS | x02 | 
| 852 | Eastern European MS-DOS	| x64| 
| 865 | Nordic MS-DOS | x66 | 
| 866 | Russian MS-DOS | x65 | 
| 874 | Thai Windows | x7C | 
| 1250 | Central European Windows | xC8 | 
| 1251 | Russian Windows | xC9 | 
| 1252 | Windows ANSI | x03 | 
| 1253 | Greek Windows	| xCB | 
| 1254 | Turkish Windows| xCA | 
| 1255 | Hebrew Windows | x7D | 
| 1256 | Arabic Windows	| x7E | 


> All encodings are converted from and to UTF-8.

## Installation
``` 
go get github.com/Valentin-Kaiser/go-dbase/dbase
```

## Projects

Projects using this package:

[👻 G(h)oST](https://github.com/Plaenkler/GoST)

## Examples

These examples can be found in the [examples](./examples/) directory:

- [Read](./examples/read/read.go)
- [Write](./examples/write/write.go)
- [Search](./examples/search/search.go)
- [Create](./examples/create/create.go)
- [Database](./examples/database/database.go)