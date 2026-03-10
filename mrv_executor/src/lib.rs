// // 引用必要的库
// use primary::Certificate;
// use crypto::Hash; // 为了能调用 .digest()
// use tokio::sync::mpsc::{Receiver, Sender};
// use std::collections::{HashMap, HashSet};
// use log::{info, warn};

// // 定义别名，让代码可读性更强
// type Round = u64;
// type Digest = crypto::Digest;

// /// MRV 执行器结构体
// /// 它的职责是：接收 Consensus 的输出 -> 缓存构建 DAG -> (未来做排序) -> 输出给 Client
// pub struct MrvExecutor {
//     // 1. 接收管道：来自 Consensus
//     rx_input: Receiver<Certificate>,
//     // 2. 发送管道：发往 Client/Analyze
//     tx_output: Sender<Certificate>,

//     // 3. 【核心记忆】全网证书仓库
//     // 只要是 Consensus 提交过的，都存这里。用来查“父子关系”。
//     store: HashMap<Digest, Certificate>,

//     // 4. 【辅助索引】按轮次归档
//     // 用来快速获取“某一轮的所有 AUF”，这是 MRV 每一轮计算的基础集合 S_w。
//     round_index: HashMap<Round, Vec<Digest>>,

//     // 5. 状态追踪：当前缓存到的最大轮次
//     max_round_stored: Round,
// }

// impl MrvExecutor {
//     /// 启动函数：这是外界调用 MRV 的入口
//     pub fn spawn(
//         rx_input: Receiver<Certificate>,
//         tx_output: Sender<Certificate>,
//     ) {
//         // 使用 Tokio 启动一个轻量级异步任务
//         tokio::spawn(async move {
//             // 初始化结构体
//             let mut executor = Self {
//                 rx_input,
//                 tx_output,
//                 store: HashMap::new(),
//                 round_index: HashMap::new(),
//                 max_round_stored: 0,
//             };
//             // 开始干活
//             executor.run().await;
//         });
//     }


//     /// 辅助函数 1：判断祖先关系 (DFS)
//     fn is_ancestor(&self, descendant_digest: &Digest, candidate_ancestor_digest: &Digest) -> bool {
//         // 1. 如果是同一个节点 -> True
//         if descendant_digest == candidate_ancestor_digest {
//             return true;
//         }

//         // 2. 获取子节点完整信息
//         let descendant_cert = match self.store.get(descendant_digest) {
//             Some(cert) => cert,
//             None => return false, // 如果连子节点都没存，肯定找不到关系
//         };

//         // 3. 获取父节点完整信息（主要是为了拿 round 做剪枝）
//         let ancestor_cert = match self.store.get(candidate_ancestor_digest) {
//             Some(cert) => cert,
//             None => return false,
//         };

//         // 4. 剪枝优化：如果子节点的轮次 <= 候选祖先的轮次，那绝对不可能是后代
//         // 因为 DAG 的边只能指向上一轮
//         if descendant_cert.round() <= ancestor_cert.round() {
//             return false;
//         }

//         // 5. 递归检查直接父节点
//         // 遍历子节点的 parents 列表，看看这些 parent 里有没有一个是目标的后代
//         for parent_digest in &descendant_cert.header.parents {
//             // 递归调用自己
//             if self.is_ancestor(parent_digest, candidate_ancestor_digest) {
//                 return true;
//             }
//         }

//         // 6. 找遍了所有路径都没找到 -> False
//         false
//     }

//     /// 辅助函数 2：计算可见性 (Seen Count)
//     /// 这里的逻辑是：在 future_round 这一轮里，有多少个 *不同作者* 的节点引用了 target_auf
//     fn calculate_seen_count(&self, target_auf: &Digest, future_round: Round) -> usize {
//         let mut seen_authors = HashSet::new();

//         // 1. 获取 future_round 这一轮所有的证书 ID
//         if let Some(future_certs) = self.round_index.get(&future_round) {
//             for future_cert_digest in future_certs {
//                 // 2. 对于这一轮的每一个证书，问：它是 target 的后代吗？
//                 if self.is_ancestor(future_cert_digest, target_auf) {
//                     // 3. 如果是，找出它的作者是谁
//                     if let Some(cert) = self.store.get(future_cert_digest) {
//                         seen_authors.insert(cert.origin()); // 插入 HashSet 自动去重
//                     }
//                 }
//             }
//         }

//         // 返回集合的大小（即有多少个不同的验证者看到了它）
//         seen_authors.len()
//     }




//     /// 主循环：流水线工人
//     async fn run(&mut self) {
//         info!("🚀 MRV Executor (V2-Basic) 已启动！");

//         while let Some(cert) = self.rx_input.recv().await {
//             let digest = cert.digest();
//             let round = cert.round();
            
//             // 1. 存入记忆 (不变)
//             self.store.insert(digest.clone(), cert.clone());
//             self.round_index
//                 .entry(round)
//                 .or_insert_with(Vec::new)
//                 .push(digest.clone());

//             if round > self.max_round_stored {
//                 self.max_round_stored = round;
//             }

//             // ================== V2 新增逻辑开始 ==================
            
//             // 2. 尝试计算 Diff (仅作演示)
//             // 假设我们要观察两轮之前的节点 (Window = 2)
//             let window_size = 2;
            
//             if round > window_size {
//                 let target_round = round - window_size;
                
//                 // 只有当这是我们在本轮收到的第一个证书时才打印（防止刷屏）
//                 let certs_in_current_round = self.round_index.get(&round).map(|v| v.len()).unwrap_or(0);
                
//                 if certs_in_current_round == 1 {
//                     // 取出目标轮次 (target_round) 的所有 AUF
//                     if let Some(target_digests) = self.round_index.get(&target_round) {
//                         // 如果这一轮至少有 2 个节点，我们就比较前两个
//                         if target_digests.len() >= 2 {
//                             let a_digest = &target_digests[0];
//                             let b_digest = &target_digests[1];
                            
//                             // 调用刚才写的函数！
//                             let seen_a = self.calculate_seen_count(a_digest, round);
//                             let seen_b = self.calculate_seen_count(b_digest, round);
                            
//                             // 计算 Diff
//                             let diff = (seen_a as i64) - (seen_b as i64);

//                             info!(
//                                 "🔍 [MRV-Calc] 站在 Round {} 看 Round {} | A被看: {} | B被看: {} | Diff: {}",
//                                 round, target_round, seen_a, seen_b, diff
//                             );
//                         }
//                     }
//                 }
//             }
//             // ================== V2 新增逻辑结束 ==================

//             // 3. 转发给 Client (不变)
//             if let Err(_) = self.tx_output.send(cert).await {
//                 break;
//             }
//         }
//     }
// }




// use primary::Certificate;
// use crypto::Hash;
// use tokio::sync::mpsc::{Receiver, Sender};
// use std::collections::{HashMap, HashSet};
// use log::{info, warn, debug};
// use std::cmp::Ordering;

// // ==========================================
// //           MRV 配置参数 (可调整)
// // ==========================================
// // 观察窗口大小：MRV 会等待未来 W 个 Round 的证据
// // 本地测试网络极快，设为 3-5 即可；真实网络建议设大一些
// const MRV_WINDOW_SIZE: u64 = 3; 

// // 拜占庭节点数 f。本地 fab local 默认是 4 个节点，所以 f=1
// const F_SIZE: usize = 1;

// // 差异阈值 Delta = f + 1
// // 只有当 A 的可见性比 B 多出至少 Delta 个时，才认为 A 优于 B
// const THRESHOLD_DELTA: i64 = (F_SIZE + 1) as i64; 

// // 类型别名
// type Round = u64;
// type Digest = crypto::Digest;

// /// MRV 排序执行器
// pub struct MrvExecutor {
//     // 通道
//     rx_input: Receiver<Certificate>,
//     tx_output: Sender<Certificate>,
    
//     // 核心数据存储
//     store: HashMap<Digest, Certificate>,
//     round_index: HashMap<Round, Vec<Digest>>,
    
//     // 状态追踪
//     max_round_stored: Round,      // 当前收到的最大轮次
//     last_finalized_round: Round,  // 上一个已经排序并输出的轮次
// }

// impl MrvExecutor {
//     /// 启动 MRV 任务
//     pub fn spawn(rx_input: Receiver<Certificate>, tx_output: Sender<Certificate>) {
//         tokio::spawn(async move {
//             let mut executor = Self {
//                 rx_input,
//                 tx_output,
//                 store: HashMap::new(),
//                 round_index: HashMap::new(),
//                 max_round_stored: 0,
//                 last_finalized_round: 0,
//             };
//             executor.run().await;
//         });
//     }

//     /// 主循环
//     async fn run(&mut self) {
//         info!("🚀 MRV Executor (V3-Full) 启动！Window={}, Delta={}", MRV_WINDOW_SIZE, THRESHOLD_DELTA);

//         while let Some(cert) = self.rx_input.recv().await {
//             let digest = cert.digest();
//             let round = cert.round();

//             // 1. 存入记忆 (Memory)
//             self.store.insert(digest.clone(), cert.clone());
//             self.round_index
//                 .entry(round)
//                 .or_insert_with(Vec::new)
//                 .push(digest.clone());

//             if round > self.max_round_stored {
//                 self.max_round_stored = round;
//             }

//             // 2. 滑动窗口触发逻辑
//             // 只有当 (当前轮次 - 窗口大小) > 上次处理轮次时，才推进窗口
//             // 举例：Window=3。当前收到 Round 10，则 safe_limit = 7。
//             // 我们可以尝试结算 Round 1, 2, ..., 7。
//             let safe_round_limit = if round > MRV_WINDOW_SIZE { round - MRV_WINDOW_SIZE } else { 0 };

//             while self.last_finalized_round < safe_round_limit {
//                 let target_round = self.last_finalized_round + 1;
                
//                 // 检查目标轮次是否有数据
//                 // 注意：这里使用 cloned() 是为了把 Vec 拿出来操作，避免借用冲突
//                 if let Some(mut target_digests) = self.round_index.get(&target_round).cloned() {
//                     if !target_digests.is_empty() {
//                         // 【关键步骤】
//                         // 必须对输入列表进行确定性排序！
//                         // 否则不同机器上的 Vec 顺序可能不同，导致两两比较的顺序不同
//                         target_digests.sort(); 

//                         info!("⚡ [MRV-Sort] 正在对 Round {} 进行公平排序... (包含 {} 个 AUF)", target_round, target_digests.len());
                        
//                         // --- 核心排序算法 ---
//                         let sorted_digests = self.mrv_sort(target_digests, target_round, round);
                        
//                         // --- 按序输出结果 ---
//                         for d in sorted_digests {
//                             if let Some(c) = self.store.get(&d) {
//                                 if let Err(_) = self.tx_output.send(c.clone()).await {
//                                     warn!("❌ 下游 Client 已断开，MRV 停止工作。");
//                                     return; 
//                                 }
//                             }
//                         }
//                     }
//                 } else {
//                     // 这一轮可能没有数据（空轮），但也算处理过了
//                     debug!("⚠️ Round {} 没有收到任何 AUF，跳过。", target_round);
//                 }
                
//                 // 标记该轮已完成，推进指针
//                 self.last_finalized_round = target_round;
//             }
//         }
//     }

//     // ==========================================
//     //           MRV 核心算法实现 (V3)
//     // ==========================================

//     /// 对一组 AUF 进行 MRV 排序
//     /// input_list: 待排序的 AUF 列表 (已在外部 sort 过，保证顺序一致)
//     /// target_round: 它们所在的轮次
//     /// current_max_round: 当前系统看到的最新轮次 (用于限定 Seen 曲线的计算范围)
//     fn mrv_sort(&self, input_list: Vec<Digest>, target_round: Round, current_max_round: Round) -> Vec<Digest> {
//         let n = input_list.len();
//         if n <= 1 { return input_list; } // 只有一个不用排

//         // --- Step 1: 构图 (Build Preference Graph) ---
//         // graph[A] = [B, C] 表示 A -> B (A优于B), A -> C
//         let mut graph: HashMap<Digest, Vec<Digest>> = HashMap::new();
//         // in_degree[A] = k 表示有 k 个节点排在 A 前面
//         let mut in_degree: HashMap<Digest, usize> = HashMap::new();
        
//         // 初始化
//         for d in &input_list {
//             graph.insert(d.clone(), Vec::new());
//             in_degree.insert(d.clone(), 0);
//         }

//         // 两两比较
//         for i in 0..n {
//             for j in (i + 1)..n {
//                 let a = &input_list[i];
//                 let b = &input_list[j];

//                 // 计算 A vs B 的公平关系
//                 let relation = self.compare_aufs(a, b, target_round, current_max_round);

//                 match relation {
//                     OrderingRelation::ABetter => {
//                         // A -> B (A 优于 B)
//                         graph.get_mut(a).unwrap().push(b.clone());
//                         *in_degree.get_mut(b).unwrap() += 1;
//                         debug!("   ⚖️  MRV 判定: {:?} -> {:?}", a, b);
//                     },
//                     OrderingRelation::BBetter => {
//                         // B -> A (B 优于 A)
//                         graph.get_mut(b).unwrap().push(a.clone());
//                         *in_degree.get_mut(a).unwrap() += 1;
//                         debug!("   ⚖️  MRV 判定: {:?} -> {:?}", b, a);
//                     },
//                     OrderingRelation::Tie => {
//                         // 平局，不加边
//                         // debug!("   ⚖️  MRV 平局: {:?} == {:?}", a, b);
//                     }
//                 }
//             }
//         }

//         // --- Step 2: 拓扑排序 (Kahn算法 + 暴力破环) ---
//         let mut final_order = Vec::new();
//         let mut active_nodes = input_list.clone(); // 还没排进序列的节点

//         while !active_nodes.is_empty() {
//             // A. 找出所有入度为 0 的节点 (Candidates)
//             let mut zero_degree_nodes: Vec<Digest> = active_nodes.iter()
//                 .filter(|d| *in_degree.get(d).unwrap() == 0)
//                 .cloned()
//                 .collect();

//             if zero_degree_nodes.is_empty() {
//                 // B. 出现环了！(Condorcet Cycle: A->B->C->A)
//                 // 此时所有剩余节点的入度都 > 0
//                 // 策略：确定性暴力破环 (Deterministic Cycle Breaking)
//                 // 规则：在剩下的节点中，找 Hash 最小的那个，强制把它“提拔”为胜者
//                 warn!("⚠️ 检测到循环偏好 (Cycle)！执行暴力破环...");
                
//                 // active_nodes 已经包含所有剩余节点
//                 // 对 active_nodes 按 Hash 排序 (保证确定性)
//                 active_nodes.sort(); 
                
//                 // 取最小的那个作为“被选中的人”，打破僵局
//                 let forced_winner = active_nodes[0].clone();
//                 zero_degree_nodes.push(forced_winner);
//             } else {
//                 // C. 正常的 Tie-break
//                 // 如果有多个入度为 0 的节点，它们之间是“平局”
//                 // 按 Hash 大小排序输出 (保证确定性)
//                 zero_degree_nodes.sort();
//             }

//             // 取出第一个胜者
//             let winner = zero_degree_nodes[0].clone();
//             final_order.push(winner.clone());

//             // 从待处理列表中移除
//             if let Some(pos) = active_nodes.iter().position(|x| *x == winner) {
//                 active_nodes.remove(pos);
//             }

//             // D. 消除影响：将 winner 指向的所有邻居入度减 1
//             if let Some(neighbors) = graph.get(&winner) {
//                 for neighbor in neighbors {
//                     if let Some(degree) = in_degree.get_mut(neighbor) {
//                         if *degree > 0 {
//                             *degree -= 1;
//                         }
//                     }
//                 }
//             }
//         }

//         final_order
//     }

//     /// 比较两个 AUF，返回关系 (A优、B优、还是平局)
//     fn compare_aufs(&self, a: &Digest, b: &Digest, start_round: Round, max_round: Round) -> OrderingRelation {
//         let mut pos = 0; // A > B 的轮数 (Evidence for A)
//         let mut neg = 0; // B > A 的轮数 (Evidence for B)

//         // 遍历未来的每一轮 (Seen Curve)
//         for r in (start_round + 1)..=max_round {
//             let seen_a = self.calculate_seen_count(a, r);
//             let seen_b = self.calculate_seen_count(b, r);
//             let diff = (seen_a as i64) - (seen_b as i64);

//             // 只有差异超过阈值 Delta 才算作有效证据
//             if diff >= THRESHOLD_DELTA {
//                 pos += 1;
//             } else if diff <= -THRESHOLD_DELTA {
//                 neg += 1;
//             }
//         }

//         // 极保守判定规则 (Conservative Rule)
//         // 只有在 "只有正面证据，且没有任何反面证据" 时才下结论
//         if pos >= 1 && neg == 0 {
//             OrderingRelation::ABetter
//         } else if neg >= 1 && pos == 0 {
//             OrderingRelation::BBetter
//         } else {
//             OrderingRelation::Tie
//         }
//     }

//     // ==========================================
//     //           辅助函数 (Helpers)
//     // ==========================================

//     /// 判断祖先关系 (DFS)
//     fn is_ancestor(&self, descendant_digest: &Digest, candidate_ancestor_digest: &Digest) -> bool {
//         if descendant_digest == candidate_ancestor_digest { return true; }
        
//         let descendant_cert = match self.store.get(descendant_digest) { Some(c) => c, None => return false };
//         let ancestor_cert = match self.store.get(candidate_ancestor_digest) { Some(c) => c, None => return false };
        
//         // 剪枝
//         if descendant_cert.round() <= ancestor_cert.round() { return false; }

//         for parent in &descendant_cert.header.parents {
//             if self.is_ancestor(parent, candidate_ancestor_digest) { return true; }
//         }
//         false
//     }

//     /// 计算可见性 (Seen Count)
//     fn calculate_seen_count(&self, target_auf: &Digest, future_round: Round) -> usize {
//         let mut seen_authors = HashSet::new();
//         if let Some(future_certs) = self.round_index.get(&future_round) {
//             for future_cert in future_certs {
//                 if self.is_ancestor(future_cert, target_auf) {
//                     if let Some(c) = self.store.get(future_cert) {
//                         seen_authors.insert(c.origin());
//                     }
//                 }
//             }
//         }
//         seen_authors.len()
//     }
// }

// // 内部枚举：比较结果
// enum OrderingRelation {
//     ABetter,
//     BBetter,
//     Tie,
// }



use primary::Certificate;
use crypto::Hash;
use tokio::sync::mpsc::{Receiver, Sender};
use std::collections::{HashMap, HashSet, BTreeMap, VecDeque};
use log::{info, warn, debug};
use std::cmp::Ordering;

// --- MRV 配置 ---
const MRV_WINDOW_SIZE: u64 = 3;
const F_SIZE: usize = 1;
const THRESHOLD_DELTA: i64 = (F_SIZE + 1) as i64;

// 类型别名
pub type Round = u64;
pub type Digest = crypto::Digest;

pub struct MrvExecutor {
    rx_input: Receiver<Certificate>,
    tx_output: Sender<Certificate>,
    
    // 核心数据
    store: HashMap<Digest, Certificate>,
    round_index: BTreeMap<Round, Vec<Digest>>,
    
    // 状态
    max_round_stored: Round,
    last_finalized_round: Round,
}

impl MrvExecutor {
    pub fn spawn(rx_input: Receiver<Certificate>, tx_output: Sender<Certificate>) {
        tokio::spawn(async move {
            let mut executor = Self {
                rx_input,
                tx_output,
                store: HashMap::new(),
                round_index: BTreeMap::new(),
                max_round_stored: 0,
                last_finalized_round: 0,
            };
            executor.run().await;
        });
    }

    async fn run(&mut self) {
        info!("🚀 MRV Executor (V4-Stable+SCC) 启动. Window={}, Delta={}", MRV_WINDOW_SIZE, THRESHOLD_DELTA);

        while let Some(cert) = self.rx_input.recv().await {
            let digest = cert.digest();
            let round = cert.round();

            self.store.insert(digest.clone(), cert.clone());
            self.round_index.entry(round).or_default().push(digest);
            
            if round > self.max_round_stored {
                self.max_round_stored = round;
            }

            let safe_round_limit = if round > MRV_WINDOW_SIZE { round - MRV_WINDOW_SIZE } else { 0 };
            
            while self.last_finalized_round < safe_round_limit {
                let target_round = self.last_finalized_round + 1;
                
                if let Some(mut target_digests) = self.round_index.get(&target_round).cloned() {
                    if !target_digests.is_empty() {
                        info!("⚡ [MRV-Sort] Round {} 开始排序 ({} AUFs)...", target_round, target_digests.len());
                        
                        // 执行排序
                        let sorted_digests = self.mrv_sort(target_digests, target_round, round);
                        
                        // 输出
                        for digest in sorted_digests {
                            if let Some(c) = self.store.get(&digest) {
                                if self.tx_output.send(c.clone()).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
                
                self.last_finalized_round = target_round;
            }
        }
    }

    // ==========================================
    //           核心算法 (SCC + Tie-Break)
    // ==========================================

    fn mrv_sort(&self, mut input_list: Vec<Digest>, target_round: Round, current_max_round: Round) -> Vec<Digest> {
        if input_list.len() <= 1 { return input_list; }

        // 1. 预排序 (使用分层规则 Tie-Break)
        input_list.sort_by(|a, b| self.tie_break(a, b));

        // 2. 构建偏好图
        let mut graph: HashMap<Digest, HashSet<Digest>> = input_list.iter().map(|d| (d.clone(), HashSet::new())).collect();
        let mut memo: HashMap<(Digest, Digest), bool> = HashMap::new(); // 缓存

        for i in 0..input_list.len() {
            for j in (i + 1)..input_list.len() {
                let a = &input_list[i];
                let b = &input_list[j];
                
                match self.compare_aufs(a, b, target_round, current_max_round, &mut memo) {
                    OrderingRelation::ABetter => { graph.get_mut(a).unwrap().insert(b.clone()); },
                    OrderingRelation::BBetter => { graph.get_mut(b).unwrap().insert(a.clone()); },
                    OrderingRelation::Tie => {}
                }
            }
        }

        // 3. 计算 SCC (强连通分量)
        let components = self.find_scc(&input_list, &graph);
        
        // 4. 构建 Component DAG
        let mut comp_graph: HashMap<usize, HashSet<usize>> = (0..components.len()).map(|i| (i, HashSet::new())).collect();
        let mut comp_indegree: HashMap<usize, usize> = (0..components.len()).map(|i| (i, 0)).collect();

        // 建立组件 id 查找表
        let mut node_to_comp = HashMap::new();
        for (idx, comp) in components.iter().enumerate() {
            for node in comp {
                node_to_comp.insert(node.clone(), idx);
            }
        }

        for (from, tos) in &graph {
            let c_from = node_to_comp[from];
            for to in tos {
                let c_to = node_to_comp[to];
                if c_from != c_to {
                    if comp_graph.get_mut(&c_from).unwrap().insert(c_to) {
                        *comp_indegree.get_mut(&c_to).unwrap() += 1;
                    }
                }
            }
        }

        // 5. 拓扑排序 (Kahn算法)
        let mut result = Vec::new();
        let mut queue: Vec<usize> = comp_indegree.iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&i, _)| i)
            .collect();
        
        // 关键：即使是 Component 之间，也要确定性排序
        queue.sort_by(|a, b| self.compare_components(&components[*a], &components[*b]));
        let mut queue = VecDeque::from(queue);

        while let Some(curr) = queue.pop_front() {
            // 将当前 Component 内的节点加入结果 (内部也要 Tie-Break)
            let mut members = components[curr].clone();
            members.sort_by(|a, b| self.tie_break(a, b));
            result.extend(members);

            if let Some(neighbors) = comp_graph.get(&curr) {
                let mut newly_ready = Vec::new();
                for &next in neighbors {
                    let deg = comp_indegree.get_mut(&next).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        newly_ready.push(next);
                    }
                }
                // 再次排序，保证确定性
                newly_ready.sort_by(|a, b| self.compare_components(&components[*a], &components[*b]));
                queue.extend(newly_ready);
            }
        }

        result
    }

    // --- 比较逻辑 ---

    fn compare_aufs(
        &self, a: &Digest, b: &Digest, start: Round, max: Round, 
        memo: &mut HashMap<(Digest, Digest), bool>
    ) -> OrderingRelation {
        let mut pos = 0;
        let mut neg = 0;

        for r in (start + 1)..=max {
            let s_a = self.calculate_seen(a, r, memo) as i64;
            let s_b = self.calculate_seen(b, r, memo) as i64;
            let diff = s_a - s_b;

            if diff >= THRESHOLD_DELTA { pos += 1; }
            else if diff <= -THRESHOLD_DELTA { neg += 1; }
        }

        if pos >= 1 && neg == 0 { OrderingRelation::ABetter }
        else if neg >= 1 && pos == 0 { OrderingRelation::BBetter }
        else { OrderingRelation::Tie }
    }

    fn calculate_seen(&self, target: &Digest, round: Round, memo: &mut HashMap<(Digest, Digest), bool>) -> usize {
        let mut seen = HashSet::new();
        if let Some(certs) = self.round_index.get(&round) {
            for c in certs {
                if self.is_ancestor(c, target, memo) {
                    if let Some(full) = self.store.get(c) {
                        seen.insert(full.origin());
                    }
                }
            }
        }
        seen.len()
    }

    fn is_ancestor(&self, desc: &Digest, anc: &Digest, memo: &mut HashMap<(Digest, Digest), bool>) -> bool {
        if desc == anc { return true; }
        
        // 修复点：这里的 Key 必须是 Owned 类型，不能是引用
        // 使用 .clone() 解决 E0507 错误
        let key = (desc.clone(), anc.clone()); 
        if let Some(&res) = memo.get(&key) { return res; }

        let d_cert = match self.store.get(desc) { Some(c) => c, None => return false };
        let a_cert = match self.store.get(anc) { Some(c) => c, None => return false };

        if d_cert.round() <= a_cert.round() {
            memo.insert(key, false);
            return false;
        }

        let found = d_cert.header.parents.iter().any(|p| self.is_ancestor(p, anc, memo));
        memo.insert(key, found);
        found
    }

    // --- 分层 Tie-Break ---
    fn tie_break(&self, a: &Digest, b: &Digest) -> Ordering {
        let ca = self.store.get(a);
        let cb = self.store.get(b);
        match (ca, cb) {
            (Some(ca), Some(cb)) => {
                ca.round().cmp(&cb.round()) // 1. Round
                .then_with(|| ca.origin().0.as_ref().cmp(cb.origin().0.as_ref())) // 2. Author
                .then_with(|| a.cmp(b)) // 3. Hash
            }
            _ => a.cmp(b),
        }
    }

    // 比较两个 Component (取各自最小元素进行比较)
    fn compare_components(&self, c1: &[Digest], c2: &[Digest]) -> Ordering {
        let min1 = c1.iter().min_by(|a, b| self.tie_break(a, b)).unwrap();
        let min2 = c2.iter().min_by(|a, b| self.tie_break(a, b)).unwrap();
        self.tie_break(min1, min2)
    }

    // --- Kosaraju 算法求 SCC ---
    fn find_scc(&self, nodes: &[Digest], graph: &HashMap<Digest, HashSet<Digest>>) -> Vec<Vec<Digest>> {
        let mut visited = HashSet::new();
        let mut stack = Vec::new();
        
        // 1. 正向 DFS
        for node in nodes {
            if !visited.contains(node) {
                self.dfs1(node, graph, &mut visited, &mut stack);
            }
        }

        // 2. 反向图
        let mut reverse_graph: HashMap<Digest, Vec<Digest>> = nodes.iter().map(|d| (d.clone(), Vec::new())).collect();
        for (u, vs) in graph {
            for v in vs {
                reverse_graph.entry(v.clone()).or_default().push(u.clone());
            }
        }

        // 3. 反向 DFS
        visited.clear();
        let mut components = Vec::new();
        while let Some(node) = stack.pop() {
            if !visited.contains(&node) {
                let mut comp = Vec::new();
                self.dfs2(&node, &reverse_graph, &mut visited, &mut comp);
                components.push(comp);
            }
        }
        components
    }

    fn dfs1(&self, u: &Digest, graph: &HashMap<Digest, HashSet<Digest>>, visited: &mut HashSet<Digest>, stack: &mut Vec<Digest>) {
        visited.insert(u.clone());
        if let Some(vs) = graph.get(u) {
            for v in vs {
                if !visited.contains(v) {
                    self.dfs1(v, graph, visited, stack);
                }
            }
        }
        stack.push(u.clone());
    }

    fn dfs2(&self, u: &Digest, rev_graph: &HashMap<Digest, Vec<Digest>>, visited: &mut HashSet<Digest>, comp: &mut Vec<Digest>) {
        visited.insert(u.clone());
        comp.push(u.clone());
        if let Some(vs) = rev_graph.get(u) {
            for v in vs {
                if !visited.contains(v) {
                    self.dfs2(v, rev_graph, visited, comp);
                }
            }
        }
    }
}

enum OrderingRelation { ABetter, BBetter, Tie }