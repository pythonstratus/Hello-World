# Activity View Bind Parameter Reference

## Group Manager Level (elevel = 6)

| ROID       | org  | elevel | levelValue | daysUpperLimit | ROID Range Matched         |
|------------|------|--------|------------|----------------|----------------------------|
| 25143500   | CF   | 6      | 251435     | 90             | 25143500 – 25143599        |
| 25143513   | CF   | 6      | 251435     | 90             | 25143500 – 25143599        |
| 22063400   | CF   | 6      | 220634     | 90             | 22063400 – 22063499        |
| 22063419   | CF   | 6      | 220634     | 90             | 22063400 – 22063499        |
| 26133700   | CF   | 6      | 261337     | 90             | 26133700 – 26133799        |
| 26133711   | CF   | 6      | 261337     | 90             | 26133700 – 26133799        |

> **Note:** 25143500 and 25143513 share the same GM (251435).  
> 22063400 and 22063419 share the same GM (220634).  
> 26133700 and 26133711 share the same GM (261337).

## RO Level (elevel = 8)

| ROID       | org  | elevel | levelValue | daysUpperLimit | ROID Range Matched         |
|------------|------|--------|------------|----------------|----------------------------|
| 25143500   | CF   | 8      | 25143500   | 90             | 25143500 only              |
| 25143513   | CF   | 8      | 25143513   | 90             | 25143513 only              |
| 22063400   | CF   | 8      | 22063400   | 90             | 22063400 only              |
| 22063419   | CF   | 8      | 22063419   | 90             | 22063419 only              |
| 26133700   | CF   | 8      | 26133700   | 90             | 26133700 only              |
| 26133711   | CF   | 8      | 26133711   | 90             | 26133711 only              |

## How the Formula Works

```
TRUNC(a.roid / POWER(10, 8 - elevel)) = levelValue
```

| elevel | POWER(10, 8-elevel) | Effect                              | Example (ROID 25143513)       |
|--------|---------------------|-------------------------------------|-------------------------------|
| 0      | 100,000,000         | National — all ROIDs                | TRUNC(25143513/100000000) = 0 |
| 2      | 1,000,000           | Area — first 2 digits               | TRUNC(25143513/1000000) = 25  |
| 4      | 10,000              | Territory — first 4 digits          | TRUNC(25143513/10000) = 2514  |
| 6      | 100                 | Group Manager — first 6 digits      | TRUNC(25143513/100) = 251435  |
| 8      | 1                   | RO — exact ROID match               | TRUNC(25143513/1) = 25143513  |
