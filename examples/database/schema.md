## Database schema 

Generated in 111.1118ms 

## employees 

- Fields: `16` 
- Records: `3` 
- File size: `2377 B`  
- First Row at: `808 B`  
- Record Length: `523` 
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Type | Length | 
| --- | --- | --- | 
| *EMPLOYEEID* | I | 4 | 
| *DEPARTMENT* | C | 50 | 
| *SOCIALSECU* | C | 30 | 
| *EMPLOYEENU* | C | 30 | 
| *FIRSTNAME* | C | 50 | 
| *LASTNAME* | C | 50 | 
| *TITLE* | C | 50 | 
| *EMAILNAME* | C | 50 | 
| *EXTENSION* | C | 30 | 
| *ADDRESS* | M | 4 | 
| *CITY* | C | 50 | 
| *STATEORPRO* | C | 20 | 
| *POSTALCODE* | C | 20 | 
| *COUNTRY* | C | 50 | 
| *WORKPHONE* | C | 30 | 
| *NOTES* | M | 4 | 

## expense_categories 

- Fields: `3` 
- Records: `5` 
- File size: `687 B`  
- First Row at: `392 B`  
- Record Length: `59` 
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Type | Length | 
| --- | --- | --- | 
| *EXPENSECAT* | I | 4 | 
| *EXPENSECA2* | C | 50 | 
| *EXPENSECA3* | I | 4 | 

## expense_details 

- Fields: `6` 
- Records: `6` 
- File size: `962 B`  
- First Row at: `488 B`  
- Record Length: `79` 
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Type | Length | 
| --- | --- | --- | 
| *EXPENSEDET* | I | 4 | 
| *EXPENSEREP* | I | 4 | 
| *EXPENSECAT* | I | 4 | 
| *EXPENSEITE* | Y | 8 | 
| *EXPENSEIT2* | C | 50 | 
| *EXPENSEDAT* | T | 8 | 

## expense_reports 

- Fields: `9` 
- Records: `3` 
- File size: `1004 B`  
- First Row at: `584 B`  
- Record Length: `140` 
- Last modified: `2022-10-15 00:00:00 +0200 CEST` 

| Field | Type | Length | 
| --- | --- | --- | 
| *EXPENSEREP* | I | 4 | 
| *EMPLOYEEID* | I | 4 | 
| *EXPENSETYP* | C | 50 | 
| *EXPENSERPT* | C | 30 | 
| *EXPENSERP2* | M | 4 | 
| *DATESUBMIT* | T | 8 | 
| *ADVANCEAMO* | Y | 8 | 
| *DEPARTMENT* | C | 30 | 
| *PAID* | L | 1 | 

