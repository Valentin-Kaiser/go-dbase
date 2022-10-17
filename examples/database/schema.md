## Database schema 

 Generated in 135.8104ms 

## EMPLOYEES 

- Fields: `16` 
- Records: `3` 
- First record: `808`  
- Record size: `523 B` 
- File size: `2.4 kB`  
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Field type | Golang type | Length | Comment | 
| --- | --- | --- | --- | --- | 
| *EMPLOYEEID* | I | int32 | 4 |  | 
| *DEPARTMENT* | C | string | 50 |  | 
| *SOCIALSECU* | C | string | 30 |  | 
| *EMPLOYEENU* | C | string | 30 |  | 
| *FIRSTNAME* | C | string | 50 |  | 
| *LASTNAME* | C | string | 50 |  | 
| *TITLE* | C | string | 50 |  | 
| *EMAILNAME* | C | string | 50 |  | 
| *EXTENSION* | C | string | 30 |  | 
| *ADDRESS* | M | []uint8 | 4 |  | 
| *CITY* | C | string | 50 |  | 
| *STATEORPRO* | C | string | 20 |  | 
| *POSTALCODE* | C | string | 20 |  | 
| *COUNTRY* | C | string | 50 |  | 
| *WORKPHONE* | C | string | 30 |  | 
| *NOTES* | M | []uint8 | 4 |  | 

## EXPENSE_CATEGORIES 

- Fields: `3` 
- Records: `5` 
- First record: `392`  
- Record size: `59 B` 
- File size: `687 B`  
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Field type | Golang type | Length | Comment | 
| --- | --- | --- | --- | --- | 
| *EXPENSECAT* | I | int32 | 4 |  | 
| *EXPENSECA2* | C | string | 50 |  | 
| *EXPENSECA3* | I | int32 | 4 |  | 

## EXPENSE_DETAILS 

- Fields: `6` 
- Records: `6` 
- First record: `488`  
- Record size: `79 B` 
- File size: `962 B`  
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Field type | Golang type | Length | Comment | 
| --- | --- | --- | --- | --- | 
| *EXPENSEDET* | I | int32 | 4 |  | 
| *EXPENSEREP* | I | int32 | 4 |  | 
| *EXPENSECAT* | I | int32 | 4 |  | 
| *EXPENSEITE* | Y | float64 | 8 |  | 
| *EXPENSEIT2* | C | string | 50 |  | 
| *EXPENSEDAT* | T | time.Time | 8 |  | 

## EXPENSE_REPORTS 

- Fields: `9` 
- Records: `3` 
- First record: `584`  
- Record size: `140 B` 
- File size: `1.0 kB`  
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Field type | Golang type | Length | Comment | 
| --- | --- | --- | --- | --- | 
| *EXPENSEREP* | I | int32 | 4 |  | 
| *EMPLOYEEID* | I | int32 | 4 |  | 
| *EXPENSETYP* | C | string | 50 |  | 
| *EXPENSERPT* | C | string | 30 |  | 
| *EXPENSERP2* | M | []uint8 | 4 |  | 
| *DATESUBMIT* | T | time.Time | 8 |  | 
| *ADVANCEAMO* | Y | float64 | 8 |  | 
| *DEPARTMENT* | C | string | 30 |  | 
| *PAID* | L | bool | 1 |  | 

