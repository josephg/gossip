import assert from 'assert/strict'

type Id = number

export type SeqRange = [number, number] // [start, end).

// Remote version.
export type Version = Map<Id, SeqRange[]>

export function pushRLE(ranges: SeqRange[], start: number, end: number) {
  assert(end > start)
  if (ranges.length > 0) {
    assert(start >= ranges[ranges.length - 1][1])
  }

  if (ranges.length > 0 && ranges[ranges.length - 1][1] == start) {
    ranges[ranges.length - 1][1] = end
  } else {
    ranges.push([start, end])
  }
}

// Version is mutated in-place.
export function pushSingle(v: Version, id: Id, seq: number) {
  let s = v.get(id)
  if (s == null) {
    v.set(id, [[seq, seq + 1]])
  } else {
    pushRLE(s, seq, seq + 1)
  }
}

// Returns true if a is a superset of b.
export function versionContainsVersion(a: Version, b: Version): boolean {
  // We're looking for any version in b that is missing from a.

  // Cheating! If a is a superset of b, b - a will be the empty set.
  return versionSubtract(b, a).size == 0
}

function vCount(v: Version): number {
  let result = 0
  for (const ranges of v.values()) {
    for (const [start, end] of ranges) {
      result += end - start
    }
  }
  return result
}

export function versionsDistinct(a: Version, b: Version): boolean {
  // This is horribly inefficient.
  let ab = versionSubtract(a, b)
  let ba = versionSubtract(b, a)
  return vCount(ab) == vCount(a) && vCount(ba) == vCount(b)
}

// Computes & returns a - b.
export function versionSubtract(a: Version, b: Version): Version {
  const result = new Map()
  for (const [id, aRanges] of a) {
    const bRanges = b.get(id)
    if (bRanges == null) {
      result.set(id, structuredClone(aRanges))
    } else {
      const r: SeqRange[] = []

      let b_i = 0
      for (let a_i = 0; a_i < aRanges.length; a_i++) {
        // Take as much as we can from this range.
        let [ar_start, ar_end] = aRanges[a_i]

        for (; b_i < bRanges.length; b_i++) {
          let [br_start, br_end] = bRanges[b_i]
          if (ar_end <= br_start) break; // B item is irrelevant.
          if (br_end <= ar_start) continue; // Skip this item in b.

          // They're guaranteed to intersect.

          if (ar_start < br_start) {
            // Take stem.
            pushRLE(r, ar_start, br_start)
          }

          // ar_start >= br_Start now.

          if (ar_end <= br_end) {
            // Stop here, leaving the b item for the next outer loop iter.
            ar_start = ar_end
            break
          } else {
            // Skip this b-item and continue.
            ar_start = br_end
          }
        }

        if (ar_start < ar_end) {
          // Push the remainder.
          pushRLE(r, ar_start, ar_end)
        }
      }

      if (r.length > 0) {
        result.set(id, r)
      }
    }
  }
  return result
}

export function versionUnion(a: Version, b: Version): Version {
  const result = new Map()

  for (const [id, bb] of b) {
    // Grab all the items that are in b but not a.
    if (!a.has(id)) {
      result.set(id, structuredClone(bb))
    }
  }

  for (const [id, aRanges] of a) {
    let bRanges = b.get(id)
    if (bRanges == null) {
      result.set(id, structuredClone(aRanges))
    } else {
      let r: SeqRange[] = []

      let a_i = 0, b_i = 0
      while (a_i < aRanges.length && b_i < bRanges.length) {
        // Take the next item.
        let [ar_start, ar_end] = aRanges[a_i]
        let [br_start, br_end] = bRanges[b_i]

        if (ar_end < br_start) {
          // Non-intersect. Take ar and iterate.
          pushRLE(r, ar_start, ar_end)
          a_i++
        } else if (br_end < ar_start) {
          pushRLE(r, br_start, br_end)
          b_i++
        } else {
          // Intersect. Take the union of both entries.
          let start = ar_start < br_start ? ar_start : br_start
          let end = ar_end > br_end ? ar_end : br_end
          pushRLE(r, start, end)
          a_i++
          b_i++

          while (a_i < aRanges.length && aRanges[a_i][1] <= end) a_i++
          while (b_i < bRanges.length && bRanges[b_i][1] <= end) b_i++
        }
      }

      for (; a_i < aRanges.length; a_i++) {
        pushRLE(r, aRanges[a_i][0], aRanges[a_i][1])
      }
      for (; b_i < bRanges.length; b_i++) {
        pushRLE(r, bRanges[b_i][0], bRanges[b_i][1])
      }

      result.set(id, r)
    }
  }

  return result
}

// debugger
// versionUnion(new Map([[1, [[0, 5]]]]), new Map([[1, [[0, 2], [4, 5]]]]))