package YTDNMgmt

import (
	"bytes"
	"reflect"
)

func EqualSorted(listA, listB interface{}) (ok bool) {
	if listA == nil || listB == nil {
		return listA == listB
	}

	aKind := reflect.TypeOf(listA).Kind()
	bKind := reflect.TypeOf(listB).Kind()

	if aKind != reflect.Array && aKind != reflect.Slice {
		return false
	}

	if bKind != reflect.Array && bKind != reflect.Slice {
		return false
	}

	aValue := reflect.ValueOf(listA)
	bValue := reflect.ValueOf(listB)

	if aValue.Len() != bValue.Len() {
		return false
	}

	// Mark indexes in bValue that we already used
	visited := make([]bool, bValue.Len())

	for i := 0; i < aValue.Len(); i++ {
		element := aValue.Index(i).Interface()

		found := false
		for j := 0; j < bValue.Len(); j++ {
			if visited[j] {
				continue
			}

			if ObjectsAreEqual(bValue.Index(j).Interface(), element) {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func ObjectsAreEqual(expected, actual interface{}) bool {

	if expected == nil || actual == nil {
		return expected == actual
	}
	if exp, ok := expected.([]byte); ok {
		act, ok := actual.([]byte)
		if !ok {
			return false
		} else if exp == nil || act == nil {
			return exp == nil && act == nil
		}
		return bytes.Equal(exp, act)
	}
	return reflect.DeepEqual(expected, actual)

}

func Max(num ...int64) int64 {
	max := num[0]
	for _, v := range num {
		if v > max {
			max = v
		}
	}
	return max
}

func Min(num ...int64) int64 {
	min := num[0]
	for _, v := range num {
		if v < min {
			min = v
		}
	}
	return min
}
