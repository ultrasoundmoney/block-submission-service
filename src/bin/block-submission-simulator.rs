use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read},
};

use anyhow::Result;
use block_submission_service::{env::ENV_CONFIG, log, BlockSubmission, STREAM_NAME};
use flate2::read::GzDecoder;
use fred::{
    prelude::{ClientLike, RedisClient, StreamsInterface},
    types::RedisConfig,
};
use tracing::{debug, info};

const STATE_ROOTS: [&str; 87] = [
    "0x02fdf00bcdbc6f0d5e81ef481b86874ad3b92511c0f2a03745f9cd8d6bf35787",
    "0x03d3e1c595fcf840b5e3f342922068d724e34eb430a6678f8fff031ab4dd5d33",
    "0x05014e5e38db33abba9b336fe35bfb6db924cf7aa669afe32c4624008786b223",
    "0x06b6ab55dd6e0787fc5a0c05c7aef07bdc33fdb9872fc6c9f6daec920d7f2999",
    "0x113f21242b29e7886eb5a76e461a71f2ff3de1ef1ef21d3268a2669fae9f0710",
    "0x155a157df6f6ff052322a34479fac7c83529edb4d2247a11636576fe5e0da738",
    "0x17909061a03e61312636b886da3b700d6c455be70fab62cc081fc8f03f1407a5",
    "0x1a070622f21b21f13668793957321fd294c0741845942608a883eeb6f032fc7f",
    "0x1b50bb3341355c56155e08f171f845ee9eb26e957c724e8df73caf20885df39d",
    "0x1c6e6d9d1732ea1032c0b5944701fd6b66fbb348cf239654dd3e0c414d7386ec",
    "0x255be1a573ddc9576292794be4011c6ab68447664213b3c3bb55824c3b9e722c",
    "0x28a328d98285298d1619b20588e57629ca1675418b517946c6c63d6ed02f93ff",
    "0x2b7a5aad085fdb8148ba5a039c148e5efae73b6571f2555ab96bf27450f5d42a",
    "0x2e2c7add6305eb526379d7bd1297a4e0d29f896a9cd271d4c707b51b053b8a5c",
    "0x4246509fe98bfcff310b74643395f9df1dcaec2ae4f3114894d188a736e19f15",
    "0x45e255d4bf0e74859d5d2e4536e482576c7b1362515ea70187a54bfa8e5be652",
    "0x4a864e08d5bcc9de4aa409b5cc3edb50f3a8cbe3b8a50f2b75c9a07aaca753b6",
    "0x4d4751bc85c538d7c21d24b7e2c54f11bc126cd4a3513a4f59c536387b23b32f",
    "0x4e6a938b4df91162db62f92f5d751176898616cb5263b66dd16e4befd4786fcf",
    "0x5742687bf9f1eab98537dade8e55e528449655537ca4da3a58fb101ce22efba2",
    "0x57a1f1724c850896ef805a5e81db06a1142e4549aea3a45a71bed20d47b1ab31",
    "0x6762230e1bee18fdd718833bba32d435fb893bd56e709ee5aeb7c93512fd62e0",
    "0x67d3acba38a2a10ea8fe06bad2594a58bd2eb7db2eef3ace2ef0f6211fad3efe",
    "0x68b4be36a6dd43b50ee97c8b59964638ebed63727c453ef2f4d234c822c9dc57",
    "0x69b1009c20690b779b110b6683457b32195a0a41a8ac37520cca7d081aa3744c",
    "0x6b320fff4bac1fcd2251057042c0c002d609245c15d2f8fc62633efc522b82df",
    "0x7f72851d58c47e13e5f95e96213c244de098c1ebb2c295db9229934e442013c6",
    "0x8088db6f5114d9782f24e7cf74ffc60f29874549f13150ab89ab74c2673d7897",
    "0x81cb2e27e2e3a0ac66b33d276617fd21da7d891f8b9bb5a9ec15678f10db7085",
    "0x833268c931ec09796b70f3b931cec71b39e17f3f41c29f65804cb474b2aa9efb",
    "0x84a1ef5f04b91b71bdf55019b8d04d16c1b0e45571cef664c718e6602d23b8a9",
    "0x89118b3b28f38abdeec268b87fc47b34a43c97fe6cc6331f507a6a29954bfab9",
    "0x8d16845248e1f1f541428aa163610e6336caba365bb60d29f5a539016c799e69",
    "0x92fac952297f2769d7d6d77f75f4c4cf33f786164e1a7672169d4a9eaacffd6e",
    "0x94f3714939395e5d9480604c0c24a5a95a1e04ff6fd838a5fe6eb678b37caf0a",
    "0x9d0e36a1fd4ba855cbcc2b17d77b70bcb4f41b22a98cd7540a2c88ac38f22da4",
    "0x9d417147d8620ee9c2a39a2e43369a3fd8793d408668bd9afba661da94908b82",
    "0x9d89f204744a3486a5e2f28ef454d3d48535f3d48daefbbffa347854fc462f4d",
    "0x9eecc9c58fb6c404e3a5702af80069b4cf8e4db2e7604e74d998d2cafd173d29",
    "0xa38730c81052a85442e2d7c5f00a52fcaf99a8e3e1a250f619c4b59ea19b5d07",
    "0xa5fb77fbed40fc4226b19afbf4412360ff0b7db27f7a3f7e5b307a954bd60998",
    "0xa6f63091b72a3b3a2364b5d372a821f3e8f65e93310690eef2fcd4a8b66a831b",
    "0xa7d88531701869558c3967e5803a93a2f8d3905f7c5dc615fd991e6e1bb33b88",
    "0xa8bcfc8e772aa481594660a72b04e0fa3cc014247d237b1e409190870bf9ef9d",
    "0xab4ca52b5fdecbfdb9836b30dcfc9c7a3c6da79f6464b3cadff86e14b92b44f4",
    "0xb147ffe7f300bf89ed62bfa982d19819f1af60a777c1424ef620a369a6450015",
    "0xb1b44bc8901a55bf28c4c862ecb71627dae3bc1ef7cef590e2e9678fcda3292b",
    "0xb4447f955d4ba4035183ac5f12368ab58160496841a096ebfeaaa027669cedb4",
    "0xb61c0df7cd9439322df6c4d8356728bb26c950d14aa324a283687dc65fe5d1e6",
    "0xbc5fe22fb08969c4e1cc6a857dff37a0aa7d27601aea4f07782fdd7d1c8b3f90",
    "0xbda4eb66e23686c4b2982f589529a8df3c2265000ebc12e2d1aec6385960d9ea",
    "0xbdeca84e24df017659590d2fc98ebbe9930616bfd56869a68a94b577c30d6583",
    "0xc084307e531c5178f3a3fdc10d03786728df867f25014d961e4491ab73bed690",
    "0xc4a818397eff6cbe74d53c695bd4ac0974fe3d28842c7368b88bfcc6a8205974",
    "0xca18d2cd142dd339ebca9d1abc4891c078119f389fd0577a7df7d4f4b2bfd87c",
    "0x003fc7725acd9365420171d5768dda1d0dd28dc50ea15126f4a4db81b2cf3dfb",
    "0x08272b1849eaea5e282835dd09e5bea4aa97ed722377e9deba77c933bb795cf0",
    "0x2d4a30b5af8831d5d537ba44f4ae5b9b801d0d2638cd1ee76b14be4807485008",
    "0x6fa16a576bccb38a4fce28fc491cdf23cae1c2f43baef6c4d5881a4ac6c623d2",
    "0x8e97c3fcd691c84cae26146ac2b3e52285d5d5d476a6c7d6c4f6316a247ffb55",
    "0x8f1046509a074afb1f5b68ba569b59c25d9be807c75b5c169f48bb791003c28d",
    "0x25ebac6ca8f11529a2059a0d2b1587311e5251ec0d9c1cb502e50f9d4241dcd2",
    "0x44e394e556f53972a17a1ccc97c8a52666be5f079c8109e0cad9028336300e1b",
    "0x84e72748640abddfea5bd6cd4eeebb1a28b719b806104651810c2d7c24461564",
    "0x184fc76f70d8ec826c227d3f78c7da70b0fd86d4cbad6ee003c9090a9d21a497",
    "0x222b573e8f3d580c6e460f898b2db8e23a765ae4a44b61bc514b306b9412f6be",
    "0x246e67217a0ce7a783275eafe9ed7198a97710466bd40e2ae91d1342b725aa69",
    "0x481ff3577552ca2edd1221b2c00a3ed12109638bc22d63fdbbd9907111db321a",
    "0x716a6496ff13621dbd3383a2272e737d2579f5aa9262ff391dc430e3882d1a4f",
    "0x968c951bdabbebdc8b09ae0c04dad391a801039267a84ff90501a7c04e9a5095",
    "0x46439af3bccd4666d517f9cbb4673e44f1888fa9a8d0785acf12c862bcc1621b",
    "0x62019eca17cc66f85b1aab3eada683a73bf93b66090622c49c21dfad9a508468",
    "0x1220127eb2e1c71b6797546d93491e7cbc29befc559c2ff6b4205370719735cd",
    "0x2603401a9651155fb9847fb6855ef3b79d59b742dc7967e4f274af32478386f9",
    "0xabb8525a5b37f80bbef560bed6e8c04dd96e766a001fdd6950db1a71cc1cc75d",
    "0xaeec32e0be1c65b28648bc5b987934023c202a60abb8a4bab0029f190b86c7e5",
    "0xb2b49fa11a4b75304c29780a1d060f1f984fa97ba7d99dfaadfb7e099a9dd996",
    "0xbe8ec96373c34ef3aed25c598dbd3e55e7b588711bf0b17280bedf2d5dfd0819",
    "0xc1f78dc8c96c307121c3919dc2981becf81c848582e3468850c50f95f4d0984d",
    "0xcbf8204de7d89dc76b63ceb65b44dfc485a65cdafe4c61c6142f7e6bdfb67ca0",
    "0xe6f8296dc2dc48affb8917981b5d350d905837f389ce48c0c68567777f8322fe",
    "0xe8621f3a85d190d91c6f012c4d4f088d6b205771a4395d46f05ccd7d05a55a39",
    "0xef97837e8ba94a53cb3da0e8913b7a7c4d81ad0f29ef9ed19d1ac00942f20a23",
    "0xf46afaa90c0de08d293476b9b92f0cbef02f35f1fbd090b6d4a5576f388d0533",
    "0xf94b40bdfcf3ecfe1867df059f7b94924a5153e4e9a484cdb9220854f14bdd75",
    "0xf574ef527d30d5c8416b7b7934f9e6cd5bb5f514d412fb392764b3223225f676",
    "0xf3121bf6f5a409dffe1c4c111111046c5465ab6733f1bc20bcef539fa6670bf6",
];

fn decompress_gz_to_file(input_path: &str, output_path: &str) -> Result<(), std::io::Error> {
    let input_file = File::open(input_path)?;
    let decoder = GzDecoder::new(input_file);
    let mut reader = BufReader::new(decoder);

    let output_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)?;

    let mut writer = BufWriter::new(output_file);

    std::io::copy(&mut reader, &mut writer)?;

    Ok(())
}

fn read_file(file_path: &str) -> Result<String, std::io::Error> {
    let mut file = File::open(file_path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content)
}

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("simulating block submissions");

    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let client = RedisClient::new(config, None, None);
    client.connect();
    client.wait_for_connect().await?;

    let input_paths = STATE_ROOTS
        .iter()
        .map(|state_root| format!("example_block_submissions/{}.json.gz", state_root));

    let inputs = STATE_ROOTS.iter().zip(input_paths);

    for (_state_root, input_path) in inputs {
        let decompressed_path = &format!("{}.decompressed", input_path);

        // Check if decompressed file exists
        if !std::path::Path::new(decompressed_path).exists() {
            debug!("decompressing {}", input_path);
            decompress_gz_to_file(&input_path, decompressed_path)?;
        }

        let raw_block_submission = read_file(decompressed_path)?;
        let block_submission: BlockSubmission = serde_json::from_str(&raw_block_submission)?;
        let slot = block_submission.slot();
        let proposer_pubkey = block_submission.proposer_pubkey();
        let block_hash = block_submission.block_hash();

        client
            .xadd(STREAM_NAME, false, None, "*", block_submission)
            .await?;

        debug!(%slot, %proposer_pubkey, %block_hash, "simulated block submission");
    }

    info!("done simulating block submissions");

    Ok(())
}
