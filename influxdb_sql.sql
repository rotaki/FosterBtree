tps =
  from(bucket: "tlb")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "t" and r._field == "v")
    |> group(columns: [])
    |> aggregateWindow(every: 1s, fn: count)
    |> map(fn: (r) => ({ r with _value: r._value * 1000, _field: "TPS" }))

read =
  from(bucket: "tlb")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "d" and r._field == "v" and r.o == "r")
    |> group(columns: [])
    |> aggregateWindow(every: 1s, fn: count)
    |> map(fn: (r) => ({ r with _value: r._value * 100, _field: "read/sec" }))

write =
  from(bucket: "tlb")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "d" and r._field == "v" and r.o == "w")
    |> group(columns: [])
    |> aggregateWindow(every: 1s, fn: count)
    |> map(fn: (r) => ({ r with _value: r._value * 100, _field: "write/sec" }))

total =
  from(bucket: "tlb")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "d" and r._field == "v")
    |> group(columns: [])
    |> aggregateWindow(every: 1s, fn: count)
    |> map(fn: (r) => ({ r with _value: r._value * 100, _field: "total/sec" }))

tps   |> yield(name: "TPS")
read  |> yield(name: "read/sec")
write |> yield(name: "write/sec")
total |> yield(name: "total/sec")