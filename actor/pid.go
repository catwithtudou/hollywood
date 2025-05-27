package actor

import (
	"github.com/zeebo/xxh3"
)

// pidSeparator 定义了 PID 层级结构中的分隔符
// 用于构建父子进程关系的标识符
const pidSeparator = "/"

// NewPID 创建一个新的进程标识符
// address: 进程所在的地址（通常是节点地址）
// id: 进程的唯一标识符
// 返回: 新创建的 PID 实例
func NewPID(address, id string) *PID {
	p := &PID{
		Address: address,
		ID:      id,
	}
	return p
}

// String 返回 PID 的字符串表示形式
// 格式: "address/id"，便于调试和日志输出
func (pid *PID) String() string {
	return pid.Address + pidSeparator + pid.ID
}

// Equals 比较两个 PID 是否相等
// 只有当 Address 和 ID 都相同时才认为两个 PID 相等
func (pid *PID) Equals(other *PID) bool {
	return pid.Address == other.Address && pid.ID == other.ID
}

// Child 创建当前 PID 的子进程 PID
// id: 子进程的标识符
// 返回: 新的子进程 PID，其 ID 为 "父进程ID/子进程ID" 的层级结构
// 这种设计支持 Actor 模型中的层级化进程管理
func (pid *PID) Child(id string) *PID {
	childID := pid.ID + pidSeparator + id
	return NewPID(pid.Address, childID)
}

// LookupKey 生成用于哈希表查找的键值
// 将 Address 和 ID 组合后使用 xxh3 算法生成 64 位哈希值
// 用于在进程注册表中快速定位和查找进程
// xxh3 是一个高性能的非加密哈希算法，适合做键值查找
func (pid *PID) LookupKey() uint64 {
	key := []byte(pid.Address)
	key = append(key, pid.ID...)
	return xxh3.Hash(key)
}
