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


data =
    from(bucket: "tlb")
        |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
        |> filter(fn: (r) =>
            r._measurement == "s20" and
            (r._field == "h" or r._field == "p" or r._field == "f") and
            (r.c == "6" or r.c == "8")
        )

// ─── 1 second sum for every field ───
data
    |> group(columns: ["_field"])        // drop “c” so 6 + 8 are combined
    |> aggregateWindow(every: 1s, fn: sum, createEmpty: false)
    |> yield(name: "sum_per_sec")

// ----------------------------------------------------------------------------
// helper to build+yield the s-series ×100 for a given ID,
// with separate lines per (c, _field)
// ----------------------------------------------------------------------------
makeAndYieldSum = (id) =>
  from(bucket: "tlb")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    // pick the right measurement (s1 or s2)
    |> filter(fn: (r) => r._measurement == "s" + id)
    // only the three sub-fields we care about
    |> filter(fn: (r) =>
         r._field == "h" or r._field == "p" or r._field == "f"
       )
    // group by both the field AND the container tag
    |> group(columns: ["_field", "c"])
    // sum each (c, field) pair per second
    |> aggregateWindow(every: 1s, fn: sum, createEmpty: false)
    // scale by 100
    |> map(fn: (r) => ({ r with _value: r._value * 100 }))
    // this single yield now emits SIX lines:
    //   c=6,h ; c=6,p ; c=6,f
    //   c=8,h ; c=8,p ; c=8,f
    |> yield(name: id + " sum_per_sec")

// ── run it for your two IDs ──
makeAndYieldSum(id: "")
// makeAndYieldSum(id: "2")

makeAndYield = (id, measPrefix, op, fieldName) =>
  from(bucket: "tlb")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) =>
         r._measurement == measPrefix + id and
         r._field == "v" and
         (op == "" or r.o == op)
       )
    |> group(columns: [])
    |> aggregateWindow(every: 1s, fn: count)
    |> map(fn: (r) => ({ r with 
         _value: r._value * 100, 
         _field: fieldName 
       }))
    |> yield(name: id + " " + fieldName)

// ----------------------------------------------------------------------------
// container 1
// ----------------------------------------------------------------------------
makeAndYield(id: "", measPrefix: "t",  op: "",  fieldName: "TPS")