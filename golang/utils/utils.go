package utils

func Abs(n int32) int32 {
	y := n >> 31
	return (n ^ y) - y
}

func Mod(n int32, m int) int {
	return int(Abs(n)) % m
}

// func CompareEndpoints(e1 *v2.Endpoints, e2 *v2.Endpoints) bool {
// 	if e1 == e2 {
// 		return true
// 	}
// 	if e1 == nil || e2 == nil {
// 		return false
// 	}

// }
