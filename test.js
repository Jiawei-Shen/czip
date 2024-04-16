import { Czip } from "./src/czip.js";

async function queryCzip(filePath, range) {
    const testCzip = new Czip(filePath);

    // const chunksInfo = await testCzip.getChunksInfo();
    // const blockInfo = await testCzip.query("chr1:3900000-4000000");
    // const blockInfo = await testCzip.queryIndex("chr1:3900000-4000000");
    const chunks = await testCzip.query(range);
    // const header = await testCzip.getHeader();

    return chunksInfo;
    // return header;
}

// local test
// const results = await queryCzip(
//     // { path: "./example_dataset/mm10_with_chrL.allc.cz" },
//     { path: "./example_dataset/FC_E17a_3C_1-1-I3-F13.cz", ref: "./example_dataset/mm10_with_chrL.allc.cz"},
//     "chr1:0-3100000"
// );

// remote test
const results = await queryCzip(
    { url: "https://wangftp.wustl.edu/~jshen/CZIP_set1/FC_E17a_3C_1-1-I3-F13.cz",
        ref:"https://wangftp.wustl.edu/~jshen/CZIP_set1/mm10_with_chrL.allc.cz" },
    "chr1:0-5000000"
);

console.log(results);
