package method

import (
	"fmt"
	"strings"
)

func generateUpdateClause(columns string) string {
	cols := []string{}
	for _, col := range strings.Split(columns, ",") {
		col = strings.TrimSpace(col)
		cols = append(cols, fmt.Sprintf("%s=VALUES(%s)", col, col))
	}
	return strings.Join(cols, ", ")
}

// 解析键值：从 (000000000000012345,000000000000000567,000000000000008901) 到 [12345,567,8901]
func parseKey(key string) []string {
	cleaned := strings.Trim(key, "()")
	parts := strings.Split(cleaned, ",")
	for i := range parts {
		// 移除前导零并转换为数字
		parts[i] = strings.TrimLeft(parts[i], "0")
		if parts[i] == "" {
			parts[i] = "0"
		}
	}
	return parts
}
