from(bucket: "tlb")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "t" and
                       r._field == "v")
  |> aggregateWindow(every: 1s, fn: count, createEmpty: false)
  |> yield(name: "per_type")

// ---- 1B. overall throughput (TPS) ----
from(bucket: "tlb")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "t" and
                       r._field == "v")
  |> group(columns: [])                          // ⬅️ removes all tags, including `k`
  |> aggregateWindow(every: 1s, fn: count)
  |> yield(name: "overall")