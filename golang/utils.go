package main

//https://thedeveloperblog.com/remove-duplicates-slice but edited
func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if !encountered[elements[v]] {
			encountered[elements[v]] = true
			result = append(result, elements[v])
		}
	}
	return result
}

// https://stackoverflow.com/a/67011816
func DivideIntoChunks(xs []string, chunkSize int) [][]string {
	if len(xs) == 0 {
		return nil
	}
	divided := make([][]string, (len(xs)+chunkSize-1)/chunkSize)
	prev := 0
	i := 0
	till := len(xs) - chunkSize
	for prev < till {
		next := prev + chunkSize
		divided[i] = xs[prev:next]
		prev = next
		i++
	}
	divided[i] = xs[prev:]
	return divided
}

func removeAlreadyInDB(new, already []string) []string {
	// we need to keep entries when they're in new and not in already

	oldEntries := make(map[string]bool, len(already))
	for _, elem := range already {
		oldEntries[elem] = true
	}
	var final []string

	// rotate throught new
	for _, elem := range new {
		// if in old
		if !oldEntries[elem] {
			final = append(final, elem)
		}
	}
	return final
}
