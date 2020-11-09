use crate::transaction_info::{Access, AccessMode, Target, TransactionInfo};
use std::collections::{HashMap, HashSet};
use web3::types::U256;

lazy_static! {
    static ref IGNORED_SLOTS: HashSet<&'static str> = {
        let mut ignored_slots: HashSet<&'static str> = Default::default();

        ignored_slots.insert("0xaa0bb10cec1fa372eb3abc17c933fc6ba863dd9e-0x97c1680d70dcb3b5276893fcd493400eb3490dbeffd6493b2f21d4fe5aa5abd2");
        ignored_slots.insert("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208-0x3ccb72d91c5c66cba135a4135963ed23443b8a63817f501517468fcd5097e277");
        ignored_slots.insert("0xea5ab833aff71bd7d280f0a0bc86423d1703288a-0x0000000000000000000000000000000000000000000000000000000000000004");
        ignored_slots.insert("0xb64ef51c888972c908cfacf59b47c1afbc0ab8ac-0xd66f2b991eea3cad395fb9c707ad91c177fa9410c1e674533307a898c5f6a47b");
        ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0x0000000000000000000000000000000000000000000000000000000000000006");
        ignored_slots.insert("0xb64ef51c888972c908cfacf59b47c1afbc0ab8ac-0x97143c46d2b9f80c3d070d2bf58c5c47f3cb55878bfc43fd46fe1a873fbf9226");
        ignored_slots.insert("0x4672bad527107471cb5067a887f4656d585a8a31-0x21761dd2378177dfe63291454369ce1c3b8b2b5d2f81ddfb1d9be453a5328228");
        ignored_slots.insert("0x93e682107d1e9defb0b5ee701c71707a4b2e46bc-0xead4cfabe6c708d1af28f356ffb657aae270c2da71cbbbe63ec9b0a373f00d1c");
        ignored_slots.insert("0xbe428c3867f05dea2a89fc76a102b544eac7f772-0xc702b582f673f86731a82f6d78ab7430488457299668ada78b227c16bd29dcb7");
        ignored_slots.insert("0x53066cddbc0099eb6c96785d9b3df2aaeede5da3-0x69ab163ed3b01e67dbeb661d1dd1319ec9a159c5c12549e6f655338a4b9f6569");
        ignored_slots.insert("0xcdb7ecfd3403eef3882c65b761ef9b5054890a47-0x4eb99cffbde58ad7b86eda82eafa43b202b5611baf9d1f38b0f15145195992bb");
        ignored_slots.insert("0x45555629aabfea138ead1c1e5f2ac3cce2add830-0xcd2637d346419f51a1ea5e3fce750b056fdc406347e1b23d0d3fc64a41f5d381");
        ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182");
        ignored_slots.insert("0xf84df2db2c87dd650641f8904af71ebfc3dde0ea-0xf010ce590e31f125b7d23726f0cf815a65d4197a31113f200c1d4736af2990e7");
        ignored_slots.insert("0x5121e348e897daef1eef23959ab290e5557cf274-0xb06acb4a5e06b1dbcd9c1273438e57068b36a9346341dc615b19c96819b8904c");
        ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0x000000000000000000000000000000000000000000000000000000000000000f");
        ignored_slots.insert("0x99a2ea239a99df408470e021c012a63ec3add514-0xf3dfa1e99cfc784d38427531a1f23365adf4dbe589af8b1f6d6e562cab24ceb8");
        ignored_slots.insert("0x1530df3e1c69501d4ecb7e58eb045b90de158873-0x0000000000000000000000000000000000000000000000000000000000000006");
        ignored_slots.insert("0x286bda1413a2df81731d4930ce2f862a35a609fe-0x70571e0937d308e48dbe6a59e67c45883ee934fe35a8749f1652cd54e334aac8");
        ignored_slots.insert("0x1530df3e1c69501d4ecb7e58eb045b90de158873-0x0000000000000000000000000000000000000000000000000000000000000008");
        ignored_slots.insert("0x1530df3e1c69501d4ecb7e58eb045b90de158873-0x0000000000000000000000000000000000000000000000000000000000000007");
        ignored_slots.insert("0xf230b790e05390fc8295f4d3f60332c93bed42e2-0x0056f441f50cf05a0802b24d58c7e7c31e242a6e0b415ca32f7153d19f66d842");
        ignored_slots.insert("0xcdcfc0f66c522fd086a1b725ea3c0eeb9f9e8814-0xf7de38d8225be9afcecda97e3fa54dec312204e1a5833324662411c69764c8f8");
        ignored_slots.insert("0xfd8971d5e8e1740ce2d0a84095fca4de729d0c16-0x2c0a98951937961e73ce108b3022409a1af103c4a007bc8ea1bd15cb9c0eb143");
        ignored_slots.insert("0xa0febbd88651ccca6180beefb88e3b4cf85da5be-0xb08fb9b4732ca221f04fd2b8042c74c2e05a7dd97837d78f601c31e67689122d");
        ignored_slots.insert("0xc8058d59e208399b76e66da1ec669dd6b1bee2ea-0x45a67118cf5b78c9d5665f32d84f5354ff4cc51ca057f9912db8001160138eb4");
        ignored_slots.insert("0xc8058d59e208399b76e66da1ec669dd6b1bee2ea-0x3a1e8083289bdd4f6d2a7f99677e470110b138bb7bc44091b55c912ed8924e3b");
        ignored_slots.insert("0xe7d3e4413e29ae35b0893140f4500965c74365e5-0x0000000000000000000000000000000000000000000000000000000000000007");
        ignored_slots.insert("0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0-0x4bccbb08c82ad34bfd3bffd66ee405411d6ef805832a887bb804a3cfec4a6894");
        ignored_slots.insert("0x95abb152ed410cc4b6dffb3ed41d01015bdbb5d2-0x23b2db8f1d36017388ae100f96d4889934e0fc87a6a97efebe1cbfac2b82ad5d");
        ignored_slots.insert("0xc5d105e63711398af9bbff092d4b6769c82f793d-0xdbb8fa1d8bbf4c430579a3dae9671ce7bd9fc8839f36ab791293a0f514ec7ec0");
        ignored_slots.insert("0xfb5a551374b656c6e39787b1d3a03feab7f3a98e-0xd2a724dd66e0ba539a8f07c13adf7b54cca29cdcd980defa3e52575599175011");
        ignored_slots.insert("0x88d50b466be55222019d71f9e8fae17f5f45fca1-0x0000000000000000000000000000000000000000000000000000000000000003");
        ignored_slots.insert("0x809826cceab68c387726af962713b64cb5cb3cca-0x91b58d999c3bb4e555b8b69e1e3e69f6302ae458ae041b58d6cd170a163bf0aa");
        ignored_slots.insert("0x10b35b348fd49966f2baf81df35a511c18bd1f80-0x0000000000000000000000000000000000000000000000000000000000000001");
        ignored_slots.insert("0x4dda7044db5fa409cc36629077ef6e56ee9a96ee-0xe818de50f1d6ce3587b95d4809295f914754258d1a4fd83e11778169f3fba775");
        ignored_slots.insert("0xce34dfae1b778d6a646b7ffba0424fd6b3d280b2-0x0000000000000000000000000000000000000000000000000000000000000000");
        ignored_slots.insert("0x5ecd84482176db90bb741ddc8c2f9ccc290e29ce-0x1746934c2068ec6c5600a84c5e8ba4bf5523d05b5eb2f3965a9d485c9e86475d");
        ignored_slots.insert("0xc5d105e63711398af9bbff092d4b6769c82f793d-0x40eb98130b64fcdf5658c4146aa2f167233acfbf4f38d8bc76666c76c817ed34");
        ignored_slots.insert("0x5385c6dc0638a9e7dc749a44b6f974d3f472aef2-0x56a2176d196fd146d383c48cfdbc37620c07e15ceee490104564af3350d81e1f");
        ignored_slots.insert("0x5ecd84482176db90bb741ddc8c2f9ccc290e29ce-0x55d0a8c155bb3462b152dac082e0704e2c1ce31add399d8cb811ceda40a3a57c");
        ignored_slots.insert("0xf230b790e05390fc8295f4d3f60332c93bed42e2-0x6695c76fcbcd7e3f85f674c835385cb06d4e7ce5b61c213e4906f18f41dbb0d1");
        ignored_slots.insert("0xb5a5f22694352c15b00323844ad545abb2b11028-0xca8de8fd833834619136f3d286dc8e8dc008b397f8ab380446dd0f1b0bc75d97");
        ignored_slots.insert("0xf0f8b0b8dbb1124261fc8d778e2287e3fd2cf4f5-0x7113310574bf33c06af834e0bab8d9576d3126889ba2c7408afd14c70146475d");
        ignored_slots.insert("0xf230b790e05390fc8295f4d3f60332c93bed42e2-0x0e0b3b0350df6dbb860eebc5bb187679879a84f10c0f05f41e40f86bef3cbaaa");
        ignored_slots.insert("0x162c91e864d37055f7d5696b845f3af520cd911d-0x2ce5e90823faa85c257ff84c9cbc9f29cb51f99aade7a4ccc22a5878ec4166a1");
        ignored_slots.insert("0x8d12a197cb00d4747a1fe03395095ce2a5cc6819-0xaa5aa2a1f1e8b58d97a3b82f0ea3de5691008176a599589980f87fdf576b8a0f");
        ignored_slots.insert("0x94ffb55ce68231c5966ea8dab16a8f066846513f-0xedffe750fb184e2be8d5e1ef73a0abd934df70b3853fc3e2a7eefb2fbdcbb1e7");
        ignored_slots.insert("0x5ecd84482176db90bb741ddc8c2f9ccc290e29ce-0xefde054c143671d351330f17d21debada551fb822864b1b4b03675eccb32f890");
        ignored_slots.insert("0x69b148395ce0015c13e36bffbad63f49ef874e03-0x075766ffeee75a49306401253b4c3f38a200fe833fa021c457955f7679c763a3");

        ignored_slots
    };
}

fn is_wr_conflict(first: &TransactionInfo, second: &TransactionInfo, allow_ignore_slots: bool) -> bool {
    for acc in second
        .accesses
        .iter()
        .filter(|a| a.mode == AccessMode::Read)
    {
        if let Target::Storage(addr, entry) = &acc.target {
            if allow_ignore_slots && IGNORED_SLOTS.contains(&format!("{}-{}", addr, entry)[..]) {
                continue;
            }

            if first.accesses.contains(&Access::storage_write(addr, entry)) {
                return true;
            }
        }
    }

    false
}

#[derive(Default)]
struct DependencyGraph {
    pub predecessors_of: HashMap<usize, Vec<usize>>,
    pub successors_of: HashMap<usize, Vec<usize>>,
}

impl DependencyGraph {
    pub fn from(txs: &Vec<TransactionInfo>, allow_ignore_slots: bool) -> DependencyGraph {
        let mut predecessors_of = HashMap::<usize, Vec<usize>>::new();
        let mut successors_of = HashMap::<usize, Vec<usize>>::new();

        for first in 0..(txs.len().saturating_sub(1)) {
            for second in (first + 1)..txs.len() {
                if is_wr_conflict(&txs[first], &txs[second], allow_ignore_slots) {
                    predecessors_of.entry(second).or_insert(vec![]).push(first);
                    successors_of.entry(first).or_insert(vec![]).push(second);
                }
            }
        }

        DependencyGraph {
            predecessors_of,
            successors_of,
        }
    }

    fn max_cost_from(&self, tx: usize, gas: &Vec<U256>, memo: &mut HashMap<usize, U256>) -> U256 {
        if let Some(result) = memo.get(&tx) {
            return result.clone();
        }

        if !self.successors_of.contains_key(&tx) {
            return gas[tx];
        }

        let max_of_successors = self.successors_of[&tx]
            .iter()
            .map(|succ| self.max_cost_from(*succ, gas, memo))
            .max()
            .unwrap_or(U256::from(0));

        let result = max_of_successors + gas[tx];
        memo.insert(tx, result);
        result
    }

    pub fn max_costs(&self, gas: &Vec<U256>) -> HashMap<usize, U256> {
        let mut memo = HashMap::new();

        (0..gas.len())
            .map(|tx| (tx, self.max_cost_from(tx, gas, &mut memo)))
            .collect()
    }

    pub fn cost(&self, gas: &Vec<U256>, num_threads: usize) -> U256 {
        let num_txs = gas.len();
        let max_cost_from = self.max_costs(gas);

        let mut threads: Vec<Option<(usize, U256)>> = vec![None; num_threads];
        let mut finished: HashSet<usize> = Default::default();

        let is_executing = |tx0: &usize, threads: &Vec<Option<_>>| {
            threads
                .iter()
                .filter_map(|opt| opt.map(|(tx1, _)| tx1))
                .find(|tx1| tx1 == tx0)
                .is_some()
        };

        let is_ready = |tx: &usize, finished: &HashSet<usize>| {
            self.predecessors_of
                .get(tx)
                .unwrap_or(&vec![])
                .iter()
                .all(|tx0| finished.contains(tx0))
        };

        let mut cost = U256::from(0);

        loop {
            // exit condition
            if finished.len() == num_txs {
                // all threads are idle
                assert!(threads.iter().all(Option::is_none));

                break;
            }

            // schedule txs on idle threads
            for thread_id in 0..threads.len() {
                if threads[thread_id].is_some() {
                    continue;
                }

                let maybe_tx = (0..num_txs)
                    .filter(|tx| !is_executing(tx, &threads))
                    .filter(|tx| !finished.contains(tx))
                    .filter(|tx| is_ready(tx, &finished))
                    .max_by_key(|tx| max_cost_from[tx]);

                let tx = match maybe_tx {
                    Some(tx) => tx,
                    None => break,
                };

                // println!("scheduling tx-{} on thread-{}", tx, thread_id);
                threads[thread_id] = Some((tx, gas[tx]));
            }

            // execute transaction
            let (thread_id, (tx, gas_step)) = threads
                .iter()
                .enumerate()
                .filter(|(_, opt)| opt.is_some())
                .map(|(id, opt)| (id, opt.unwrap()))
                .min_by_key(|(_, (_, gas))| gas.clone())
                .unwrap();

            // println!("finish executing tx-{} on thread-{}", tx, thread_id);

            threads[thread_id] = None;
            finished.insert(tx);
            cost += gas_step;

            // update gas costs
            for ii in 0..threads.len() {
                if let Some((_, gas_left)) = &mut threads[ii] {
                    *gas_left -= gas_step;
                }
            }
        }

        cost
    }
}

pub fn cost(txs: &Vec<TransactionInfo>, gas: &Vec<U256>, num_threads: usize, allow_ignore_slots: bool) -> U256 {
    DependencyGraph::from(txs, allow_ignore_slots).cost(gas, num_threads)
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::{convert_args, hashmap};

    #[rustfmt::skip]
    #[test]
    fn test_cost() {
        let mut graph = DependencyGraph::default();

        // 0 (1) - 1 (1)
        // 2 (1) - 3 (1) - 4 (1) - 5 (1)
        // 6 (1) - 7 (1)

        graph.predecessors_of = hashmap! { 1 => vec![0], 3 => vec![2], 4 => vec![3], 5 => vec![4], 7 => vec![6] };
        graph.successors_of = hashmap! { 0 => vec![1], 2 => vec![3], 3 => vec![4], 4 => vec![5], 6 => vec![7] };

        let gas = vec![1.into(); 8];
        let expected = convert_args!(values=Into::into, hashmap! ( 0 => 2, 1 => 1, 2 => 4, 3 => 3, 4 => 2, 5 => 1, 6 => 2, 7 => 1 ));

        assert_eq!(graph.max_costs(&gas), expected);

        // 0 1 6 7
        // 2 3 4 5

        assert_eq!(graph.cost(&gas, 2), U256::from(4));

        // ----------------------------------------

        // 0 (1) - 1 (3)
        // 2 (1) - 3 (1) - 4 (1) - 5 (1)
        // 6 (1) - 7 (3)

        let gas = vec![1, 3, 1, 1, 1, 1, 1, 3].into_iter().map(Into::into).collect();
        let expected = convert_args!(values=Into::into, hashmap! ( 0 => 4, 1 => 3, 2 => 4, 3 => 3, 4 => 2, 5 => 1, 6 => 4, 7 => 3 ));

        assert_eq!(graph.max_costs(&gas), expected);

        // 0 1 1 1 2 4
        // 6 7 7 7 3 5

        assert_eq!(graph.cost(&gas, 2), U256::from(6));

        // ----------------------------------------

        // 0 (1) - 1 (1) \
        // 2 (1) - 3 (1) - 4 (1) - 5 (1)
        // 6 (1) - 7 (1) /

        graph.predecessors_of.insert(4, vec![1, 3, 7]);

        graph.successors_of.insert(1, vec![4]);
        graph.successors_of.insert(7, vec![4]);

        let gas = vec![U256::from(1); 8];
        let expected = convert_args!(values=Into::into, hashmap! ( 0 => 4, 1 => 3, 2 => 4, 3 => 3, 4 => 2, 5 => 1, 6 => 4, 7 => 3 ));

        assert_eq!(graph.max_costs(&gas), expected);

        // 0 6 1 4 5
        // 2 3 7

        assert_eq!(graph.cost(&gas, 2), U256::from(5));

        // ----------------------------------------

        let graph = DependencyGraph::default();

        // 0 (1)
        // 1 (1)
        // 2 (1)
        // 3 (1)
        // 5 (1)

        let gas = vec![U256::from(1); 5];

        assert_eq!(graph.cost(&gas, 4), U256::from(2));
    }
}
