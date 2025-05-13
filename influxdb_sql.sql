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


import "experimental"

// returns the earliest _time in <bucket>/<measurement>
firstTime = (bucket, measurement) => {
  rec =
    from(bucket: bucket)
      |> range(start: 0)                     // Unix-epoch → now
      |> filter(fn: (r) => r._measurement == measurement and r._field == "v")
      |> first()
      |> findRecord(fn: (key) => true, idx: 0)

  return rec._time                           // <-- scalar time
}

    
countPerSec =
  (bucket, measurement, extraFilter, multiplier, outField) =>
    from(bucket: bucket)
      |> range(
          start: firstTime(bucket: bucket, measurement: measurement),
          stop:  v.timeRangeStop,
        )
      |> filter(fn: (r) =>                // keep only the rows you want
          r._measurement == measurement and
          r._field == "v"
        )
      |> filter(fn: extraFilter)
      |> group(columns: [])
      |> aggregateWindow(every: 1s, fn: count)
      |> map(fn: (r) => ({ r with
            _value: r._value * multiplier,
            _field: outField,
          }))
    |> experimental.alignTime(alignTo: 2021-01-01T00:00:00Z)

// ── one run (TPS + read + write + total) ────────────────────────────────────
metricsForPair = (bucket, tMeas, dMeas, suffix = "") => {
  tps =
    countPerSec(
      bucket:      bucket,
      measurement: tMeas,
      extraFilter: (r) => true,
      multiplier:  1000,
      outField:    "TPS",
    )

  read =
    countPerSec(
      bucket:      bucket,
      measurement: dMeas,
      extraFilter: (r) => r.o == "r",
      multiplier:  100,
      outField:    "read/sec",
    )

  write =
    countPerSec(
      bucket:      bucket,
      measurement: dMeas,
      extraFilter: (r) => r.o == "w",
      multiplier:  100,
      outField:    "write/sec",
    )

  total =
    countPerSec(
      bucket:      bucket,
      measurement: dMeas,
      extraFilter: (r) => true,
      multiplier:  100,
      outField:    "total/sec",
    )

  tps   |> yield(name: "TPS"        + suffix)
  // read  |> yield(name: "read/sec"   + suffix)
  // write |> yield(name: "write/sec"  + suffix)
  // total |> yield(name: "total/sec"  + suffix)

  return true
  // return union(tables: [tps, read, write, total])
}

// ── compare multiple runs ───────────────────────────────────────────────────
metricsForPair(bucket: "tlb", tMeas: "t1", dMeas: "d1", suffix: "_run1")
metricsForPair(bucket: "tlb", tMeas: "t2", dMeas: "d2", suffix: "_run2")
metricsForPair(bucket: "tlb", tMeas: "t3", dMeas: "d3", suffix: "_run3")
metricsForPair(bucket: "tlb", tMeas: "t4", dMeas: "d4", suffix: "_run4")
metricsForPair(bucket: "tlb", tMeas: "t5", dMeas: "d5", suffix: "_run5")
metricsForPair(bucket: "tlb", tMeas: "t6", dMeas: "d6", suffix: "_run6")
metricsForPair(bucket: "tlb", tMeas: "t7", dMeas: "d7", suffix: "_run7")
metricsForPair(bucket: "tlb", tMeas: "t8", dMeas: "d8", suffix: "_run8")