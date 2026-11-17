package tools

import (
	"fmt"
	"regexp"
	"strings"
)

// exec_guard.go 把 exec / host_exec 的危险命令闸从"5 条子串黑名单"升级为
// 两级分类闸（2026-11 复现驱动的重构）：
//
//   - execBlocked：不可逆的大规模破坏（递归删除根/家目录/系统目录、写裸设备、
//     fork 炸弹、--no-preserve-root 等）。无条件拒绝，force=true 也不能放行。
//   - execConfirm：可逆但有危险信号的操作（远程管道执行、持久化机制、
//     git 强推/硬重置、关机、写 shell 配置）。模型必须用 exec(force=true)
//     重发一次才执行 —— 确认动作留在对话记录里，等于签署了一份
//     "我知道这在干什么"的凭据。
//
// 为什么是分词而不是子串：旧实现 strings.Contains("rm -rf /") 可以被
// "rm -r -f /"、"cd / && rm -rf ."、换目标 "~/" 等平凡变体绕过
// （2026-11-12 的黑名单绕过复现测试里 11 条全部穿透）。分词后 "rm" 是
// 命令、"r/rf" 是递归旗标、"/" 是删除目标，三者独立判断，与书写顺序、
// 空格数量、是否有前置 cd 无关。
//
// 已知取舍（有意为之，不是遗漏）：
//   - 带空格的系统路径（"C:\Program Files"）无法作为单 token 匹配，
//     靠 "c:" 盘根目标兜底；
//   - PowerShell 别名 "ri"（Remove-Item）不识别 —— 与普通单词撞车，
//     误报代价大于漏报；
//   - curl|bash 这类装机手法对运维者是合理需求，落在 execConfirm 而不是
//     execBlocked：拦得住访客（他们本来就被强制沙箱），又不把主人的
//     正常装机变成不可能任务。

// execVerdict 是安全闸对一条命令的裁决。
type execVerdict int

const (
	execAllowed execVerdict = iota // 放行
	execConfirm                    // 可疑：需要 force=true 显式确认
	execBlocked                    // 不可逆破坏：无条件拒绝
)

// String 方便日志与测试输出。
func (v execVerdict) String() string {
	switch v {
	case execBlocked:
		return "blocked"
	case execConfirm:
		return "confirm"
	default:
		return "allowed"
	}
}

// normalizedCommand 是归一化（小写 + 折叠空白 + 去行继续符）后的命令，
// 所有匹配都在归一化串上做，破 "rm  -R  -f" 这类空白变体。
type normalizedCommand string

func normalizeExecCommand(cmd string) normalizedCommand {
	s := strings.ToLower(cmd)
	s = strings.ReplaceAll(s, "\\\n", " ") // 行继续符
	s = strings.ReplaceAll(s, "\r", " ")
	// shell 分隔符归一化：&& → ;，独立 &（后台）→ ;。
	// 只动带空格邻接的形式，URL 里的 a=1&b=2 不受影响。
	s = strings.ReplaceAll(s, "&&", " ; ")
	s = strings.ReplaceAll(s, " & ", " ; ")
	s = strings.TrimSuffix(s, " &")
	return normalizedCommand(strings.Join(strings.Fields(s), " "))
}

// segments 按 ; | 换行与括号切分，每个片段独立判断。
// "cd / ; rm -rf *" 切出 "rm -rf *"；"(rm -rf /)" 切出 "rm -rf /"。
func (n normalizedCommand) segments() []string {
	return strings.FieldsFunc(string(n), func(r rune) bool {
		switch r {
		case ';', '|', '\n', '(', ')':
			return true
		}
		return false
	})
}

// execSegment 是一个归一化后的命令片段及其 token。
type execSegment struct {
	raw    string
	tokens []string
}

// eachSegment 对每个片段跑 fn，取最严裁决；blocked 即短路。
// 传给 fn 的 token 已剥掉 sudo/env 这类前缀包装。
func (n normalizedCommand) eachSegment(fn func(seg execSegment) execVerdict) execVerdict {
	worst := execAllowed
	for _, s := range n.segments() {
		toks := strings.Fields(s)
		if len(toks) == 0 {
			continue
		}
		if v := fn(execSegment{raw: s, tokens: stripExecPrefix(toks)}); v > worst {
			worst = v
		}
		if worst == execBlocked {
			return worst // 已到顶，不必再看
		}
	}
	return worst
}

// ---- 递归删除（rm / Remove-Item / del|rd / find -delete / chmod|chown -R）----

// recursiveDeleteDangerousTargets 是"删了就回不来"的目标 token 集合。
// 精确 token 匹配："/var/log/nginx" 不在其中，正常清理不受影响；
// "/"、"~"、"*"、系统目录在其中，清这类缓存请写全路径。
var recursiveDeleteDangerousTargets = map[string]bool{
	"/": true, "/*": true, "/.": true, "/..": true,
	"~": true, "~/*": true, "~/": true, "~/.": true,
	"$home": true, "$home/*": true, "${home}": true, "${home}/*": true,
	".": true, "..": true, "./*": true, "../*": true, "*": true,
	"c:": true, "c:\\": true, "c:/": true, "c:\\*": true, "c:/*": true,
	"d:": true, "e:": true,
	"/users": true, "/users/*": true, "/home": true, "/home/*": true,
	"/etc": true, "/usr": true, "/var": true, "/boot": true, "/opt": true,
	"/bin": true, "/sbin": true, "/lib": true, "/lib64": true,
	"/system": true, "/windows": true, "/dev": true, "/proc": true, "/sys": true,
}

// isRecursiveRmFlag 判断单个旗标是否表达递归。
// 单横线组合旗标（-rf / -fr）逐字符看（限短包，防 "-recurse" 撞车）；
// 长旗标精确/前缀匹配。
func isRecursiveRmFlag(flag string) bool {
	if strings.HasPrefix(flag, "--") {
		return flag == "--recursive" || strings.HasPrefix(flag, "--recursive=")
	}
	f := strings.TrimLeft(flag, "-")
	if f == "r" {
		return true
	}
	if len(f) > 1 && !strings.Contains(f, "=") {
		// 短组合旗标（-rf / -frv）：所有字符都是 rm 的已知旗标才逐字符判；
		// 长单词型单横线旗标（-recurse，PowerShell/部分工具风格）整词认。
		if len(f) <= 3 {
			for _, c := range f {
				if !strings.ContainsRune("rfidvn", c) {
					return false
				}
			}
			return strings.ContainsRune(f, 'r')
		}
		return f == "recurse"
	}
	return false
}

// deleteTargets 返回命令 token 里所有非旗标目标。
func deleteTargets(tokens []string) []string {
	var targets []string
	for _, t := range tokens[1:] {
		if !strings.HasPrefix(t, "-") {
			targets = append(targets, t)
		}
	}
	return targets
}

// execPrefixCommands 会"伪装"命令头：sudo rm -rf / 的第一个 token 是
// sudo，不剥掉就永远轮不到 rm 的判断。
var execPrefixCommands = map[string]bool{
	"sudo": true, "doas": true, "nohup": true,
	"env": true, "nice": true, "timeout": true, "command": true,
}

// stripExecPrefix 剥掉前缀包装（sudo / doas / nohup / env / nice /
// timeout / command），连同它们的旗标和取值（sudo -u www-data …），
// 让后面的分类器拿到真实命令头。找不到真实命令头就原样返回。
func stripExecPrefix(tokens []string) []string {
	for len(tokens) > 0 {
		head := tokens[0]
		if !execPrefixCommands[head] {
			return tokens
		}
		tokens = tokens[1:]
		switch head {
		case "timeout", "nice":
			// 第一个非旗标 token 是时长/优先级值（5、10s），一并剥掉
			if len(tokens) > 0 && !strings.HasPrefix(tokens[0], "-") && execPrefixValueRe.MatchString(tokens[0]) {
				tokens = tokens[1:]
			}
		case "env":
			// 连续的 VAR=val 赋值对都剥掉
			for len(tokens) > 0 && strings.Contains(tokens[0], "=") && !strings.HasPrefix(tokens[0], "-") {
				tokens = tokens[1:]
			}
		}
		// 通用旗标扫除（sudo -u root、doas -n …）：旗标连同取值一起剥
		for len(tokens) > 0 && strings.HasPrefix(tokens[0], "-") {
			tokens = tokens[1:]
			if len(tokens) > 0 && !strings.HasPrefix(tokens[0], "-") {
				tokens = tokens[1:]
			}
		}
	}
	return tokens
}

// execPrefixValueRe 匹配 timeout/nice 的时长/优先级值（5、10s、1m30s）。
var execPrefixValueRe = regexp.MustCompile(`^\d+[a-z]?(?:m?\d*[a-z]?)?$`)

// classifyDeleteSegment 覆盖 rm / remove-item / del|erase|rd|rmdir /
// find -delete / chmod|chown -R 的"递归 + 危险目标"组合。
// 返回 execConfirm 表示"家目录子树递归删除"——用户自己的数据，
// 一次 force=true 就能过；返回 execBlocked 才是不可逆的系统级破坏。
func classifyDeleteSegment(seg execSegment) execVerdict {
	tokens := stripExecPrefix(seg.tokens)
	if len(tokens) == 0 {
		return execAllowed
	}
	head := tokens[0]
	switch {
	case head == "rm" || head == "remove-item":
		recursive := false
		for _, f := range tokens[1:] {
			if strings.HasPrefix(f, "-") && isRecursiveRmFlag(f) {
				recursive = true
				break
			}
		}
		if !recursive {
			return execAllowed // rm -f file.txt 这类定点删除不拦
		}
		verdict := execAllowed
		for _, t := range deleteTargets(tokens) {
			if recursiveDeleteDangerousTargets[t] {
				return execBlocked
			}
			if isHomeSubtreeTarget(t) && verdict < execConfirm {
				verdict = execConfirm
			}
		}
		return verdict
	case head == "del" || head == "erase" || head == "rd" || head == "rmdir":
		// Windows del 的 /s 才是递归；/q 只是安静模式
		recursive := false
		for _, f := range tokens[1:] {
			if f == "/s" {
				recursive = true
				break
			}
		}
		if !recursive {
			return execAllowed
		}
		for _, t := range deleteTargets(tokens) {
			if recursiveDeleteDangerousTargets[t] ||
				strings.HasPrefix(t, "c:\\") || strings.HasPrefix(t, "c:/") {
				return execBlocked
			}
		}
	case head == "find":
		hasDelete := false
		hasFilter := false
		startPath := ""
		for _, t := range tokens[1:] {
			switch {
			case t == "-delete":
				hasDelete = true
			case strings.HasPrefix(t, "-"):
				// -name / -path / -iname 等过滤条件让删除变得定点
				if t == "-name" || t == "-path" || t == "-iname" || t == "-wholename" || t == "-regex" {
					hasFilter = true
				}
			case startPath == "":
				startPath = t
			}
		}
		if !hasDelete {
			return execAllowed
		}
		// find 的起点判断分两档：
		//   绝对危险起点（/、~、系统目录）→ 无条件拦，有过滤也拦
		//     （find / -name x -delete 照样能砸穿系统目录）；
		//   相对起点（.）→ 只有"裸 -delete 无过滤"才拦，
		//     find . -name '*.tmp' -delete 是正常清理，不拦。
		if startPath != "." && startPath != "./" && startPath != ".." && startPath != "./*" && startPath != "../*" && startPath != "*" &&
			recursiveDeleteDangerousTargets[startPath] {
			return execBlocked
		}
		if (startPath == "." || startPath == "./" || startPath == "") && !hasFilter {
			return execBlocked
		}
	case head == "chmod" || head == "chown":
		recursive := false
		for _, f := range tokens[1:] {
			if f == "-r" || f == "--recursive" { // 归一化已小写，-R 也落到这
				recursive = true
				break
			}
		}
		if !recursive {
			return execAllowed
		}
		for _, t := range deleteTargets(tokens) {
			// chmod/chown 的目标里混着 mode/user，宽松一点：
			// 危险目标 token 出现在任何非旗标位置就拦
			if recursiveDeleteDangerousTargets[t] {
				return execBlocked
			}
		}
	}
	return execAllowed
}

// isHomeSubtreeTarget 识别"用户家目录子树"目标（~/x、$HOME/x、
// /users/x、/home/x）。整棵家目录本身由 dangerous targets 表拦截，
// 这里只接住子目录 —— 删用户数据可逆与否存疑，降到 confirm 档。
func isHomeSubtreeTarget(t string) bool {
	for _, p := range []string{"~/", "$home/", "${home}/", "/users/", "/home/"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// ---- 裸设备与文件系统 ----

func classifyDiskSegment(seg execSegment) execVerdict {
	if len(seg.tokens) == 0 {
		return execAllowed // empty segment (tokenizer artifact) — nothing to classify
	}
	head := seg.tokens[0]
	switch {
	case head == "mkfs" || strings.HasPrefix(head, "mkfs."),
		head == "wipefs", head == "blkdiscard",
		head == "format", head == "format-volume":
		return execBlocked
	case head == "fdisk", head == "parted", head == "sgdisk", head == "gdisk":
		// 分区工具只在真指到磁盘时才是灾难
		if strings.Contains(seg.raw, "/dev/") {
			return execBlocked
		}
	case head == "dd":
		for _, t := range seg.tokens[1:] {
			if strings.HasPrefix(t, "if=/dev/") || strings.HasPrefix(t, "of=/dev/") {
				return execBlocked
			}
		}
	case head == "shutdown", head == "reboot", head == "poweroff", head == "halt":
		// 主人管理主机是合理的，但值得一次 force 确认（掉线就是掉线）
		return execConfirm
	}
	return execAllowed
}

// ---- 持久化 / 远程代码 / 不可逆 VCS ----

// ---- 整条命令级模式（片段切分会把管道撕开，这些必须整条判）----

var diskDevRedirectRe = regexp.MustCompile(`(?:>>?>?|of=)\s*/dev/(?:sd|hd|nvme|mmcblk|vd|disk)`)
var forkBombRe = regexp.MustCompile(`:\(\)\s*\{`)
var remotePipeRe = regexp.MustCompile(`(?:^|\s)(?:sudo\s+)?(?:curl|wget|curl\.exe|wget\.exe|iwr|invoke-webrequest)\s+(?:-\S+\s+)*\S+\s*\|\s*(?:sudo\s+)?(?:ba|z|da|k|fi)?sh\b`)
var cmdSubShellFetchRe = regexp.MustCompile(`(?:\$\(|<\()\s*(?:curl|wget)\s`)

// classifyExecCommand 是 exec / host_exec 共用的安全闸入口。
// 返回裁决 + 一句给人看（也给模型看）的原因。
func classifyExecCommand(cmd string) (execVerdict, string) {
	n := normalizeExecCommand(cmd)

	// --no-preserve-root 出现即拦截（唯一用途就是让 rm 真的删得动 /）
	if n.eachSegment(func(seg execSegment) execVerdict {
		for _, t := range seg.tokens {
			if t == "--no-preserve-root" {
				return execBlocked
			}
		}
		return execAllowed
	}) == execBlocked {
		return execBlocked, "--no-preserve-root"
	}

	// fork 炸弹：":(){ :|:& }" 含管道与括号，片段化会撕碎，整条匹配
	if forkBombRe.MatchString(string(n)) && strings.Contains(string(n), "|:&") {
		return execBlocked, "fork bomb"
	}
	// 重定向/写入裸设备（"> /dev/sda"、dd of=/dev/…）
	if diskDevRedirectRe.MatchString(string(n)) {
		return execBlocked, "redirect into a raw disk device"
	}
	// 远程管道执行（curl … | bash）与 $()/马上执行（bash <(curl …)）
	if remotePipeRe.MatchString(string(n)) || cmdSubShellFetchRe.MatchString(string(n)) {
		return execConfirm, "remote code execution pipe"
	}

	// 第一级：不可逆破坏（blocked）与家目录子树递归删除（confirm）
	if v := n.eachSegment(classifyDeleteSegment); v == execBlocked {
		return execBlocked, "irreversible recursive deletion on a root/home/system/wildcard target"
	} else if v == execConfirm {
		return execConfirm, "recursive deletion inside the user's home directory"
	}
	if v := n.eachSegment(classifyDiskSegment); v == execBlocked {
		return execBlocked, "raw disk / filesystem destruction"
	} else if v == execConfirm {
		return execConfirm, "power state change (shutdown/reboot)"
	}

	// 第二级：可疑（持久化 / 强推 / 配置写入）
	if n.eachSegment(classifySuspiciousSegment) == execConfirm {
		return execConfirm, "persistence, force-push or shell-config write"
	}

	return execAllowed, ""
}

func classifySuspiciousSegment(seg execSegment) execVerdict {
	// 写 shell 配置 / authorized_keys：重定向、tee/dd、cp/mv/install 都算
	if strings.Contains(seg.raw, "authorized_keys") && (strings.ContainsAny(seg.raw, ">") || tokenIn(seg, "tee", "dd", "cp", "mv", "install")) {
		return execConfirm
	}
	if shellRcWriteRe.MatchString(seg.raw) {
		return execConfirm
	}

	if len(seg.tokens) >= 2 {
		switch seg.tokens[0] {
		case "schtasks":
			return execConfirm
		case "reg", "reg.exe":
			if seg.tokens[1] == "add" || seg.tokens[1] == "delete" {
				return execConfirm
			}
		case "systemctl":
			if seg.tokens[1] == "enable" || seg.tokens[1] == "mask" {
				return execConfirm
			}
		case "launchctl":
			if seg.tokens[1] == "load" || seg.tokens[1] == "enable" {
				return execConfirm
			}
		case "update-rc.d", "chkconfig":
			return execConfirm
		case "crontab":
			// crontab -l 是只读的
			if seg.tokens[1] != "-l" {
				return execConfirm
			}
		case "iex", "invoke-expression":
			return execConfirm
		case "git":
			switch seg.tokens[1] {
			case "push":
				for _, f := range seg.tokens[2:] {
					// --force-with-lease 是带保护的强推，不算
					if f == "--force" || f == "-f" {
						return execConfirm
					}
				}
			case "reset":
				for _, f := range seg.tokens[2:] {
					if f == "--hard" {
						return execConfirm
					}
				}
			case "clean":
				for _, f := range seg.tokens[2:] {
					if strings.HasPrefix(f, "-") && !strings.HasPrefix(f, "--") && strings.Contains(f, "f") {
						return execConfirm
					}
				}
			}
		case "kill":
			// kill -9 -1（全进程团灭）要确认；kill -HUP 这类日常信号不碰
			has9, hasAll := false, false
			for _, t := range seg.tokens[1:] {
				if strings.HasPrefix(t, "-") && strings.Contains(t, "9") {
					has9 = true
				}
				if t == "-1" || t == "--" && len(seg.tokens) > 2 && seg.tokens[len(seg.tokens)-1] == "-1" {
					hasAll = true
				}
			}
			if has9 && hasAll {
				return execConfirm
			}
		}
	}
	return execAllowed
}

func tokenIn(seg execSegment, want ...string) bool {
	for _, t := range seg.tokens {
		for _, w := range want {
			if t == w {
				return true
			}
		}
	}
	return false
}

// shellRcWriteRe：重定向写 shell 配置（">> ~/.bashrc"、"> /etc/profile"）。
// 纯读取（cat ~/.bashrc）前面没有 >，不会命中。
var shellRcWriteRe = regexp.MustCompile(`>>?>?\s*\S*\.(?:bashrc|zshrc|profile|bash_profile)\b`)

// execGateError 把裁决翻译成给模型看的错误文案。
// blocked 无条件拒绝；confirm 给出 force=true 的明确出路。
func execGateError(cmd string, v execVerdict, why string) error {
	switch v {
	case execBlocked:
		return fmt.Errorf("dangerous command blocked: %s (%s). This refuses irreversible mass destruction; "+
			"run a narrower command instead, or use sandbox:true to keep it inside the container", cmd, why)
	case execConfirm:
		return fmt.Errorf("potentially dangerous command needs explicit confirmation: %s (%s). "+
			"If you are sure this is what the task needs, re-send the same call with force=true; "+
			"otherwise pick a safer approach", cmd, why)
	default:
		return nil
	}
}
