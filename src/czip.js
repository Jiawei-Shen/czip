import filehandle from "generic-filehandle";
const { LocalFile, RemoteFile, BlobFile } = filehandle;
import fetch from "node-fetch";
import { Buffer } from "buffer";
import { unzip } from "@gmod/bgzf-filehandle";
import zlib from "zlib";
import pako from "pako";

//todo: 1. sc==0/1, mc and cov type.

//The length is defined by the bytes number, 1 hex number is 4 bits, 1 byte = 2 hex number = 8 bits.
const MAGIC_LENGTH = 6;
// 2 hex numbers, 1 byte
const VERSION_LENGTH = 1;
// 2 hex numbers, 1 byte
const VERSION_MINOR_LENGTH = 1;
// 2 hex numbers, 1 byte
const SC_LENGTH = 1;
// 8 hex numbers, 4 byte
const I_ASSEMBLY_LENGTH = 4;
// I_assembly
let ASSEMBLY_TEXT_LENGTH;
// 8 hex numbers, 4 byte
const I_TEXT_LENGTH = 4;
// I_assembly
let HEADER_TEXT_LENGTH;
// n_ref 4 hex numbers, 2 byte
const N_REFS_LENGTH = 2;
const L_NAME_LENGTH = 4;
const REF_LEN_LENGTH = 4;
const MC_RECORD_SIZE = 10;

const MADEUP_HEADER_SIZE = 5000;

class VirtualOffset {
    constructor(blockOffset, blockAddress) {
        this.blockOffset = blockOffset; // < offset into the decompressed data
        this.blockAddress = blockAddress; // < offset of the compressed data block
    }
    toString() {
        return `within_block: ${this.blockAddress}, block_offset: ${this.blockOffset}`;
    }
}

class Czip {
    constructor(args) {
        this.config = args;
        this.ref = args.ref;
        if (args.path) {
            this.path = args.path;
            this.czipFileHandle = new LocalFile(this.path);

            this.czipRefHandle = new LocalFile(this.ref);
            // this.indexFile = new LocalFile(this.path + ".bci");
        } else if (args.blob) {
            this.czipFileHandle = new BlobFile(args.blob);
        } else if (args.url) {
            this.url = args.url;
            this.remote = true;
            this.czipFileHandle = new RemoteFile(this.url, { fetch });
            this.czipRefHandle = new RemoteFile(this.ref, { fetch });
            // this.indexUrl = args.url + ".bci";
            // this.indexFile = new RemoteFile(this.indexUrl, { fetch });
        } else {
            throw Error("Arguments must include blob, path, or url.");
        }

        // this.indexFile = this.indexUrl ? new RemoteFile(this.indexUrl, { fetch }) : null;
        this.header = null;
        this.chunkInfo = null;
        // this.indexData = null;
        this.initiated = false; // if the file has been initiated, header and index data are loaded
    }

    async getHeader() {
        const headerBuff = Buffer.allocUnsafe(MADEUP_HEADER_SIZE);
        const { headerBytesRead } = await this.czipFileHandle.read(headerBuff, 0, MADEUP_HEADER_SIZE, 0);
        const czipHeader = this.parseHeader(headerBuff, false);
        return czipHeader;
    }

    async getIndexHeader() {
        const headerBuff = Buffer.allocUnsafe(MADEUP_HEADER_SIZE);
        const { headerBytesRead } = await this.czipRefHandle.read(headerBuff, 0, MADEUP_HEADER_SIZE, 0);
        const czipHeader = this.parseHeader(headerBuff, true);
        return czipHeader;
    }

    async queryIndex(chrRange) {
        this.indexHeader = await this.getIndexHeader();
        // const tempBuff = Buffer.alloc(this.header.total_size);
        // console.log(this.header.total_size);
        // await this.czipFileHandle.read(tempBuff, 0, this.header.total_size, 0);

        const inputChrRange = new ChrRange(chrRange);
        const chunksInfo = await this.getChunksInfo(true);

        //Only ["chrom"] demo is build.
        const targetDim = chunksInfo.find(obj => obj.list_dimensions[0] === inputChrRange.chr)
        const parsedVirtualOffset = targetDim.list_block_offsets.map(this.parseVirtualOffset);

        const startBIndex = await this.findFirstBelowByVirtualOffset(parsedVirtualOffset, inputChrRange.start, true);
        const endBIndex = await this.findFirstAboveByVirtualOffset(parsedVirtualOffset, inputChrRange.end, true);

        const [records, startID, endID] = await this.queryRegions(true, inputChrRange, startBIndex, endBIndex, parsedVirtualOffset);
        return [records, startID, endID];
    }

    async query(chrRange) {
        this.header = await this.getHeader();
        let [mc_records, startID, endID] = await this.queryIndex(chrRange);
        // const tempBuff = Buffer.alloc(this.header.total_size);
        // console.log(this.header.total_size);
        // await this.czipFileHandle.read(tempBuff, 0, this.header.total_size, 0);

        const inputChrRange = new ChrRange(chrRange);
        const chunksInfo = await this.getChunksInfo();

        //Only ["chrom"] demo is build.
        const targetDim = chunksInfo.find(obj => obj.list_dimensions[0] === inputChrRange.chr)
        const parsedVirtualOffset = targetDim.list_block_offsets.map(this.parseVirtualOffset);
        const formatBytes = this.header.listColFormats.map(f => lenBytes(f));
        const sumBytes = formatBytes.reduce((accumulator, currentValue) => accumulator + currentValue, 0);

        // Here should be later substitute with query Index
        // const startBIndex = await this.findFirstBelowByVirtualOffset(parsedVirtualOffset, inputChrRange.start);
        // const endBIndex = await this.findFirstAboveByVirtualOffset(parsedVirtualOffset, inputChrRange.end);
        
        //Start ID and End ID
        // const startID = 347573;
        // const endID = 386520;

        const records = await this.queryByID(parsedVirtualOffset, targetDim, startID, endID, sumBytes);
        console.log("Done!");
        // const records = await this.queryRegions(inputChrRange, startBIndex, endBIndex, parsedVirtualOffset);
    }

    async getIndexData() {
        const buf = await this.indexFile.readFile();
        const bytes = await unzip(buf);
        return bytes.toString("hex");
    }

    async queryByID(virtualOffsets, chunk, startID, endID, lenBytes=2){
        // let records = [];
        const chunkStartIndex = parseInt(lenBytes * startID / 65535);
        const chunkEndIndex = parseInt(lenBytes * endID / 65535);
        const chunkStartAddress = lenBytes * startID - 65535 * chunkStartIndex;
        const chunkEndAddress = lenBytes * endID - 65535 * chunkEndIndex;
        
        let decompressedRecords = new Uint8Array(0);
        for(let i=chunkStartIndex; i <= chunkEndIndex; i++){
            let decompressedBlock;
            if(i === chunkStartIndex){
                decompressedBlock = await this.decompressBlocks(virtualOffsets[i].blockOffset, chunkStartAddress);
            }else if(i === chunkEndIndex){
                decompressedBlock = await this.decompressBlocks(virtualOffsets[i].blockOffset, 0, chunkEndAddress);
            } else {
                decompressedBlock = await this.decompressBlocks(virtualOffsets[i].blockOffset);
            }
            decompressedRecords = this.pushToUint8Array(decompressedRecords, ...decompressedBlock);
            // decompressedRecords.push(...decompressedBlock);
        }
        const records = this.parseRecord(this.header.listColFormats, this.header.listColNames, decompressedRecords, 0, decompressedRecords.length);
        console.log("Done");
    }

    async decompressBlocks(blockOffset, beginOffset=0, endOffset=65535){
        const blockMagicBuff = await this.requireBuffer(this.czipFileHandle, 2, blockOffset);
        const blockMagic = blockMagicBuff.toString();
        if(blockMagic != "MB"){
            throw new Error("Block Format Cropped!");
        }
        const blockLengthBuff = await this.requireBuffer(this.czipFileHandle, 2, blockOffset+2);
        const blockLength = parseUInt16LE(blockLengthBuff);
        // const blockLength = 120;
        const blockBuf = await this.requireBuffer(this.czipFileHandle, blockLength, blockOffset+4);
        const recordBin = pako.inflateRaw(blockBuf);

        const targetBlock = recordBin.subarray(beginOffset, endOffset);
        return targetBlock
    }

    pushToUint8Array(original, ...newElements) {
        // Create a new Uint8Array with additional space for the new elements
        const newArray = new Uint8Array(original.length + newElements.length);

        // Copy the contents of the original array into the new array
        newArray.set(original);

        // Set the new elements in the new array
        newArray.set(newElements, original.length);

        return newArray;
    }

    parseHeader(headerBuffer, index) {
        let header = {};
        let positionNow = 0;
        const magicBuf = headerBuffer.slice(positionNow, positionNow + 5);
        header["magic"] = magicBuf.toString("utf8");
        positionNow += 5;

        const versionBuf = headerBuffer.slice(positionNow, positionNow + 4);
        // header["version"] = versionBuf.toString("utf8");
        header["version"] = readFloat32(versionBuf);
        positionNow += 4;

        const total_sizeBuf = headerBuffer.slice(positionNow, positionNow + 8);
        // header["version"] = versionBuf.toString("utf8");
        header["total_size"] = parseUInt64LE(total_sizeBuf);
        positionNow += 8;

        const message_lenBuf = headerBuffer.slice(positionNow, positionNow + 2);
        header["message_len"] = parseUInt16LE(message_lenBuf);
        positionNow += 2;

        const messageBuf = headerBuffer.slice(positionNow, positionNow + header["message_len"]);
        header["message"] = messageBuf.toString("utf8");
        positionNow += header["message_len"];

        const ncolsBuf = headerBuffer.slice(positionNow, positionNow + 1);
        header["ncols"] = parseUInt8(ncolsBuf);
        positionNow += 1;

        let listColFormats = [];
        for(let i=0; i<header["ncols"]; ++i){
            const format_lenBuf = headerBuffer.slice(positionNow, positionNow + 1);
            const format_len = parseUInt8(format_lenBuf);
            positionNow += 1;

            const formatBuf = headerBuffer.slice(positionNow, positionNow + format_len);
            const format = formatBuf.toString("utf8");
            positionNow += format_len;

            listColFormats.push(format);
        }
        header["listColFormats"] = listColFormats;

        let listColNames = [];
        for(let i=0; i<header["ncols"]; ++i){
            const col_lenBuf = headerBuffer.slice(positionNow, positionNow + 1);
            const col_len = parseUInt8(col_lenBuf);
            positionNow += 1;

            const columnBuf = headerBuffer.slice(positionNow, positionNow + col_len);
            const column = columnBuf.toString("utf8");
            positionNow += col_len;
            listColNames.push(column);
        }
        header["listColNames"] = listColNames;

        const n_dimBuf = headerBuffer.slice(positionNow, positionNow + 1);
        header["n_dim"] = parseUInt8(n_dimBuf);
        positionNow += 1;

        let listDimensions = [];
        for(let i=0; i<header["n_dim"]; i++){
            const dname_lenBuf = headerBuffer.slice(positionNow, positionNow + 1);
            const dname_len = parseUInt8(dname_lenBuf);
            positionNow += 1;

            const dimBuf = headerBuffer.slice(positionNow, positionNow + dname_len);
            const dim = dimBuf.toString("utf8");
            positionNow += dname_len;
            listDimensions.push(dim);
        }
        header["listDimensions"] = listDimensions;
        header["header_size"] = positionNow

        if(index){
            this.indexHeader = header;
        } else {
            this.header = header;
        }

        return header;
    }

    async getChunksInfo(index=false){
        // if(this.header === null){
        //     await this.getHeader()
        // }
        let header;
        let fileHandle;
        if(index){
            header = this.indexHeader;
            fileHandle = this.czipRefHandle;
        } else {
            header = this.header;
            fileHandle = this.czipFileHandle;
        }

        const chunkBuff = Buffer.allocUnsafe(200);
        const { headerBytesRead } = await fileHandle.read(chunkBuff, 0, header['header_size']+100, this.header['header_size']);

        let chunkIndexed = [header['header_size']];
        let positionNow = header['header_size'];
        // const chunkInfo = await this.parseChunk(positionNow);
        let chunksInfoList = [];

        while (positionNow < header['total_size']) {
            const chunkInfo = await this.parseChunk(positionNow, index);
            chunksInfoList.push(chunkInfo);
            positionNow += chunkInfo["chunk_total_size"];
            // console.log(chunksInfoList.length);
        }
        this.chunkInfo = chunksInfoList;
        return chunksInfoList

        // const magicStringIndexes = this.findAllMagicStrings(chunkBuff, "MC")
        // let r = false;
    }

    async parseChunk(chunkOffset=0, index){
        let header;
        let fileHandle;
        if(index){
            header = this.indexHeader;
            fileHandle = this.czipRefHandle;
        } else {
            header = this.header;
            fileHandle = this.czipFileHandle;
        }

        let chunkInfo = {};
        chunkInfo["chunk_begin_offset"] = chunkOffset;
        const chunkHeadBuff = await this.requireBuffer(fileHandle, 10, chunkOffset)
        const chunk_magic = chunkHeadBuff.slice(0, 0 + 2).toString();
        if(chunk_magic !== "MC"){
            throw Error("File Cropped, chunk_magic error!")
        } else {
            chunkInfo["chunk_magic"] = chunk_magic;
        }
        const chunk_size = parseUInt64LE(chunkHeadBuff.slice(2, 2 + 8));
        chunkInfo["chunk_size"] = chunk_size;
        //block info
        const block = {};


        //Chunk Tail first two items
        const chunkTailBuff_first_two_items = await this.requireBuffer(fileHandle, 16, chunkOffset + chunk_size)
        const chunk_data_len = parseUInt64LE(chunkTailBuff_first_two_items.slice(0, 8));
        chunkInfo["chunk_data_len"] = chunk_data_len;

        const n_blocks = parseUInt64LE(chunkTailBuff_first_two_items.slice(8, 8 + 8));
        chunkInfo["n_blocks"] = n_blocks;

        //Chunk Tail list of block offsets
        const chunkTailBuff_list_block_offsets = await this.requireBuffer(fileHandle, n_blocks*8, chunkOffset + chunk_size + 16);
        let list_block_offsets = [];
        for(let i=0; i<n_blocks; i++){
            const block_offset = parseUInt64LE(chunkTailBuff_list_block_offsets.slice(i*8, i*8 + 8));
            list_block_offsets.push(block_offset)
        }
        chunkInfo["list_block_offsets"] = list_block_offsets;

        //Chunk Tail list of dimensions
        const chunkTailBuff_list_dimensions = await this.requireBuffer(fileHandle, n_blocks*8, chunkOffset + chunk_size + 16 + n_blocks * 8);
        let list_dimensions = [];
        let positionListDimentions = 0;
        for(let i=0; i<header["n_dim"]; i++){
            const dname_len = parseUInt8(chunkTailBuff_list_dimensions.slice(positionListDimentions, positionListDimentions + 1));
            positionListDimentions += 1;
            const dim_name = chunkTailBuff_list_dimensions.slice(positionListDimentions, positionListDimentions + dname_len).toString();
            positionListDimentions += dname_len;
            list_dimensions.push(dim_name)
        }
        chunkInfo["list_dimensions"] = list_dimensions;
        chunkInfo["chunk_total_size"] = chunk_size + 16 + n_blocks * 8 + positionListDimentions;
        chunkInfo["chunk_end_offset"] = chunkOffset + chunk_size + 16 + n_blocks * 8 + positionListDimentions;

        return chunkInfo
    }

    async requireBuffer(fileHandle, length, position){
        const tempBuff = Buffer.alloc(length);
        await fileHandle.read(tempBuff, 0, length, position);
        return tempBuff
    }

    parseVirtualOffset(virtualOffset){
        const virtualOffBuffer = Buffer.alloc(8);
        virtualOffBuffer.writeBigUInt64LE(BigInt(virtualOffset), 0);

        // Extract the first 2 bytes (uoffset)
        const uoffsetBuffer = virtualOffBuffer.slice(0, 2);
        const uoffset = uoffsetBuffer.readUInt16LE(0); // Assuming uoffset is a 16-bit unsigned integer

        // Extract the last 6 bytes (coffset)
        const coffsetBuffer = virtualOffBuffer.slice(2);
        const coffset = coffsetBuffer.readIntLE(0, 6);

        return new VirtualOffset(coffset, uoffset);
    }

    async parseBlock(index, blockOffset, beginOffset=0, endOffset=12){
        let header;
        let fileHandle;
        if(index){
            header = this.indexHeader;
            fileHandle = this.czipRefHandle;
        } else {
            header = this.header;
            fileHandle = this.czipFileHandle;
        }

        const blockMagicBuff = await this.requireBuffer(fileHandle, 2, blockOffset);
        const blockMagic = blockMagicBuff.toString();
        if(blockMagic != "MB"){
            throw new Error("Block Format Cropped!");
        }
        const blockLengthBuff = await this.requireBuffer(fileHandle, 2, blockOffset+2);
        const blockLength = parseUInt16LE(blockLengthBuff);
        // const blockLength = 120;
        const blockBuf = await this.requireBuffer(fileHandle, blockLength, blockOffset+4);
        const recordBin = pako.inflateRaw(blockBuf);
        const listColFormats = header.listColFormats;
        const listColNames = header.listColNames;
        const records = this.parseRecord(listColFormats, listColNames, recordBin, beginOffset, endOffset);

        // const blockBuf = Buffer.alloc(blockLength);
        // const { allBytesRead } = await this.czipFileHandle.read(blockBuf, 0, blockLength, blockOffset+4);
        // const testString = blockBuf.toString();
        return records
    }

    parseRecord(listColFormats, listColNames, recordBin, offsetBegin=0, offsetEnd){
        let offsetNow = offsetBegin;
        let records = [];
        const formatBytes = listColFormats.map(f => lenBytes(f));
        const sumBytes = formatBytes.reduce((accumulator, currentValue) => accumulator + currentValue, 0);
        while((offsetNow < offsetEnd)&&(offsetNow <= (recordBin.length - sumBytes))){
            let record = {};
            for (let i = 0; i < listColNames.length; i++) {
                const bytesLen = lenBytes(listColFormats[i]);
                const recordValue = parseDataTypes(recordBin.subarray(offsetNow, offsetNow + bytesLen), listColFormats[i], 0);
                offsetNow += bytesLen;
                record[listColNames[i]] = recordValue;
            }
            records.push(record);
        }
        return records
    }

    findFirstAbove(array, a) {
        let start = 0;
        let end = array.length - 1;
        let result = -1; // Default to -1 if no element is found

        while (start <= end) {
            let mid = Math.floor((start + end) / 2);
            //todo: Change this pos to the dim[...].
            if (array[mid][0].pos <= a) {
                // If the middle element is less than or equal to 'a', discard the left half
                start = mid + 1;
            } else {
                // If the middle element is greater than 'a', remember the index and go left to find the first element
                result = mid;
                end = mid - 1;
            }
        }

        return result;
    }

    async findFirstAboveByVirtualOffset(array, a, index) {
        let start = 0;
        let end = array.length - 1;
        let result = -1; // Default to -1 if no element is found

        while (start <= end) {
            let mid = Math.floor((start + end) / 2);
            //todo: Change this pos to the dim[...]. And 12(?).
            const theOffsets = await this.parseBlock(index, array[mid].blockOffset, array[mid].blockAddress, array[mid].blockAddress + 12)
            if (theOffsets[0].pos <= a) {
                // If the middle element is less than or equal to 'a', discard the left half
                start = mid + 1;
            } else {
                // If the middle element is greater than 'a', remember the index and go left to find the first element
                result = mid;
                end = mid - 1;
            }
        }

        return result;
    }

    async findFirstBelowByVirtualOffset(array, a, index) {
        let start = 0;
        let end = array.length - 1;
        let result = 0; // Default to -1 if no element is found

        while (start <= end) {
            let mid = Math.floor((start + end) / 2);
            //todo: Change this pos to the dim[...].
            const theOffsets = await this.parseBlock(index, array[mid].blockOffset, array[mid].blockAddress, array[mid].blockAddress + 12)
            if (theOffsets[0].pos >= a) {
                // If the middle element is greater than or equal to 'a', discard the right half
                end = mid - 1;
            } else {
                // If the middle element is less than 'a', remember the index and go right to find the first element below
                result = mid;
                start = mid + 1;
            }
        }

        return result;
    }

    async queryRegions(index, inputChrRange, startIndex, endIndex, parsedVirtualOffset){
        let records = [];
        let beforeStart = new Uint8Array(0);
        let startID = 0;
        let endID = 0;
        let header;
        let fileHandle;
        if(index){
            header = this.indexHeader;
            fileHandle = this.czipRefHandle;
        } else {
            header = this.header;
            fileHandle = this.czipFileHandle;
        }


        if(startIndex === endIndex){
            //todo: Change the pos to dim[...]
            const record = await this.parseBlock(header, parsedVirtualOffset[startIndex].blockOffset, parsedVirtualOffset[startIndex].blockAddress, 65536);
            const filteredRecord = record.filter(object => object.pos >= inputChrRange.start);
            return filteredRecord;
            // records.push(...filteredRecord);
        } else if(startIndex + 1 === endIndex){
            const recordStart = await this.parseBlock(header, parsedVirtualOffset[startIndex].blockOffset, parsedVirtualOffset[startIndex].blockAddress, 65536);
            records.push(...recordStart);
            const recordEnd = await this.parseBlock(header, parsedVirtualOffset[endIndex].blockOffset, parsedVirtualOffset[endIndex].blockAddress, 65536);
            records.push(...recordEnd);
            const filteredRecord = records.filter(object => (object.pos >= inputChrRange.start) && (object.pos <= inputChrRange.end));
            return filteredRecord;
        } else{
            for(let i=startIndex; i<endIndex; i++){
                // const tmpRecord = await this.parseBlock(parsedVirtualOffset[i].blockOffset, parsedVirtualOffset[i].blockAddress, 65536);
                const [tmpRecord, leftoverBin] = await this.parseSequentialBlocks(index, parsedVirtualOffset[i].blockOffset, parsedVirtualOffset[i].blockAddress, 65535, beforeStart);
                beforeStart = leftoverBin;

                if(i === startIndex){
                    const filteredRecord = tmpRecord.filter(object => object.pos >= inputChrRange.start);
                    startID = tmpRecord.length - filteredRecord.length;
                    records.push(...filteredRecord);
                } else if(i === endIndex - 1){
                    const filteredRecord = tmpRecord.filter(object => object.pos <= inputChrRange.end);
                    endID = filteredRecord.length;
                    records.push(...filteredRecord);
                } else{
                    records.push(...tmpRecord);
                }
            }
            // const recordStart = await this.parseBlock(parsedVirtualOffset[startIndex].blockOffset, parsedVirtualOffset[startIndex].blockAddress, 65536);
            // const recordEnd = await this.parseBlock(parsedVirtualOffset[endIndex].blockOffset, parsedVirtualOffset[endIndex].blockAddress, 65536);
        }
        const formatBytes = header.listColFormats.map(f => lenBytes(f));
        const sumBytes = formatBytes.reduce((accumulator, currentValue) => accumulator + currentValue, 0);
        startID += (startIndex * 65535 + parsedVirtualOffset[startIndex].blockAddress) / sumBytes;
        endID += ((endIndex-1) * 65535 + parsedVirtualOffset[endIndex-1].blockAddress) / sumBytes - 1;
        return [records, startID, endID];
    }

    async parseSequentialBlocks(index, blockOffset, beginOffset=0, endOffset=65535, beforeStart=[]){
        let header;
        let fileHandle;
        if(index){
            header = this.indexHeader;
            fileHandle = this.czipRefHandle;
        } else {
            header = this.header;
            fileHandle = this.czipFileHandle;
        }
        const blockMagicBuff = await this.requireBuffer(fileHandle, 2, blockOffset);
        const blockMagic = blockMagicBuff.toString();
        if(blockMagic != "MB"){
            throw new Error("Block Format Cropped!");
        }
        const blockLengthBuff = await this.requireBuffer(fileHandle, 2, blockOffset+2);
        const blockLength = parseUInt16LE(blockLengthBuff);
        // const blockLength = 120;
        const blockBuf = await this.requireBuffer(fileHandle, blockLength, blockOffset+4);
        let recordBin = pako.inflateRaw(blockBuf);
        // beforeStart.push(...recordBin);
        const combinedUint8Array = new Uint8Array(beforeStart.length + recordBin.length);
        // Copy the first array into the start of the combined array
        combinedUint8Array.set(beforeStart);
        // Copy the second array into the combined array, starting at the end of the first array
        combinedUint8Array.set(recordBin, beforeStart.length);

        const listColFormats = header.listColFormats;
        const listColNames = header.listColNames;

        let beginOffsetNew = 0;
        if(beforeStart.length === 0){
            beginOffsetNew = beginOffset;
        }
        const records = this.parseRecord(listColFormats, listColNames, combinedUint8Array, beginOffsetNew, endOffset+beforeStart.length);

        const formatBytes = listColFormats.map(f => lenBytes(f));
        const sumBytes = formatBytes.reduce((accumulator, currentValue) => accumulator + currentValue, 0);
        const testNumber = ((65535 - beginOffset) % sumBytes);
        let leftoverBin = combinedUint8Array.slice(-((65535 - beginOffset) % sumBytes));
        if(testNumber === 0){
            leftoverBin = new Uint8Array(0);
        }

        // const blockBuf = Buffer.alloc(blockLength);
        // const { allBytesRead } = await this.czipFileHandle.read(blockBuf, 0, blockLength, blockOffset+4);
        // const testString = blockBuf.toString();
        return [records, leftoverBin]
    }

}

function lenBytes(dataType){
    if(dataType ==="Q"){
        return 8
    } else if(dataType ==="f"){
        return 4
    } else if(dataType ==="H"){
        return 2
    } else if(dataType ==="B"){
        return 1
    } else if(dataType ==="c"){
        return 1
    }
    else if(dataType.includes("s")){
        return parseInt(dataType.replace(/s/g, ''))
    } else {
        return null
    }
}

function parseDataTypes(buffer, dataType, offset){
    if(dataType.includes("Q")){
        return parseUInt64LE(buffer, offset)
    } else if(dataType.includes("f")){
        return readFloat32(buffer, offset)
    } else if(dataType.includes("H")){
        return parseUInt16LE(buffer, offset)
    } else if(dataType.includes("B")){
        return parseUInt8(buffer, offset)
    } else if(dataType.includes("s")){
        const bufferString = Buffer.from(buffer);
        return bufferString.toString('utf8', offset);
    } else if(dataType.includes("c")){
        const bufferString = Buffer.from(buffer);
        return bufferString.toString('utf8', offset);
    } else {
        return null
    }
}

// Parse a <f (little-endian unsigned long long integer) from a buffer
function readFloat32(buffer, offset = 0, littleEndian = true) {
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    return view.getFloat32(offset, littleEndian);
}

// Parse a <Q (little-endian unsigned long long integer) from a buffer
function parseUInt64LE(buffer, offset= 0) {
    // const low = buffer.readUInt32LE(offset); // Read the low 32 bits
    // const high = buffer.readUInt32LE(offset + 4); // Read the high 32 bits
    // return low + high * 2**32; // Combine to form the 64-bit unsigned integer

    const dataView = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    // Parse the BigInt from the DataView using little-endian byte order
    return Number(dataView.getBigInt64(offset, true));
}

// Parse a <H (little-endian unsigned short integer) from a buffer
function parseUInt16LE(buffer, offset = 0) {
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    return view.getUint16(offset, true); // true indicates little-endian
}

// Parse a <B (unsigned char) from a buffer
function parseUInt8(buffer, offset = 0) {
    return buffer[offset];
}

class ChrRange {
    constructor(chrRange) {
        const [chromosome, start, end] = this.splitString(chrRange);
        this.chr = chromosome;
        this.start = parseInt(start);
        this.end = parseInt(end);
        this.checkStartEnd(); // Call the function upon instantiation
    }

    checkStartEnd() {
        if (this.start >= this.end) {
            throw new Error("Start value cannot be greater than end value!");
        }
        return 0;
    }

    splitString(chrRange) {
        const parts = chrRange.replace(":", "-").split("-");
        // const {header, positionNow} = viewHeaderBAIIC(hexString);
        // Extract the components
        const chromosome = parts[0];
        const start = parseInt(parts[1]);
        const end = parseInt(parts[2]);
        return [chromosome, start, end];
    }
}

function reviseDuplicates(headerRefs, list, start, end) {
    const uniqueList = [];
    list.forEach((item) => {
        if (!hasItem(uniqueList, item) && start <= item.pos && end >= item.pos) {
            // console.log(item['ref_id']);
            item['chr'] = headerRefs[item['ref_id']]['ref_name'];
            uniqueList.push(item);
        }
    });
    uniqueList.sort((a, b) => a.pos - b.pos);
    return uniqueList;
}

function hasItem(list, currentItem) {
    const index = list.findIndex(item => currentItem.pos === item.pos);
    if(index === -1){
        return false
    } else {
        return true
    }
}

function reg_to_bin(beg, end) {
    end -= 1;
    if (beg >> 14 === end >> 14) {
        return ((1 << 15) - 1) / 7 + (beg >> 14);
    }
    if (beg >> 17 === end >> 17) {
        return ((1 << 12) - 1) / 7 + (beg >> 17);
    }
    if (beg >> 20 === end >> 20) {
        return ((1 << 9) - 1) / 7 + (beg >> 20);
    }
    if (beg >> 23 === end >> 23) {
        return ((1 << 6) - 1) / 7 + (beg >> 23);
    }
    if (beg >> 26 === end >> 26) {
        return ((1 << 3) - 1) / 7 + (beg >> 26);
    }
    return 0;
}

function findIndexesByRefName(array, x) {
    return array
        .map((item, index) => (item["ref_name"] === x ? index : -1)) // Map each item to its index if ref_name matches x
        .filter((index) => index !== -1); // Filter out indexes with -1 (indicating no match)
}

function hex_to_int(hex_string) {
    // Ensure the length of the hex string is even
    if (hex_string.length % 2 !== 0) {
        throw new Error("Hex string must have an even length");
    }

    // Split the hex string into pairs of characters
    const hex_pairs = [];
    for (let i = 0; i < hex_string.length; i += 2) {
        hex_pairs.push(hex_string.slice(i, i + 2));
    }

    // Join the reversed pairs and convert to integer
    const reversed_hex_string = hex_pairs.reverse().join("");
    return parseInt(reversed_hex_string, 16);
}

function addTrailingZeros(str, length) {
    while (str.length < length) {
        str += "0";
    }
    return str;
}

function binHexMinusOne(binHex) {
    const binInt = hex_to_int(binHex) - 1;
    return addTrailingZeros(int_to_hex(binInt), binHex.length);
}

function binHexAddOne(binHex) {
    const binInt = hex_to_int(binHex) + 1;
    return addTrailingZeros(int_to_hex(binInt), binHex.length);
}

function int_to_hex(int_string) {
    let hex_string = int_string.toString(16);

    // Ensure the length of the hex string is even
    if (hex_string.length % 2 !== 0) {
        hex_string = "0" + hex_string;
    }

    // Split the hex string into pairs of characters
    const hex_pairs = [];
    for (let i = 0; i < hex_string.length; i += 2) {
        hex_pairs.push(hex_string.slice(i, i + 2));
    }

    // Join the reversed pairs and convert to integer
    const reversed_hex_string = hex_pairs.reverse().join("");
    return reversed_hex_string;
}

function hex_to_utf8(hex_string) {
    // Ensure the length of the hex string is even
    if (hex_string.length % 2 !== 0) {
        throw new Error("Hex string must have an even length");
    }

    // Split the hex string into pairs of characters
    const hex_pairs = [];
    for (let i = 0; i < hex_string.length; i += 2) {
        hex_pairs.push(hex_string.slice(i, i + 2));
    }

    // Convert each pair to a character and join them to form a string
    const utf8_string = hex_pairs.map((hex_pair) => String.fromCharCode(parseInt(hex_pair, 16))).join("");

    return utf8_string;
}

function viewHeaderBAIIC(file_content) {
    let header = {};
    let positionNow = 0;
    const magicBuf = file_content.slice(positionNow, positionNow + MAGIC_LENGTH);
    header["magic"] = magicBuf.toString("utf8");
    positionNow += MAGIC_LENGTH;

    const versionBuf = file_content.slice(positionNow, positionNow + VERSION_LENGTH);
    header["version"] = versionBuf[0];
    positionNow += VERSION_LENGTH;

    const version_minorBuf = file_content.slice(positionNow, positionNow + VERSION_MINOR_LENGTH);
    header["version_minor"] = version_minorBuf[0];
    positionNow += VERSION_MINOR_LENGTH;

    const scBuf = file_content.slice(positionNow, positionNow + SC_LENGTH);
    header["sc"] = scBuf[0];
    positionNow += SC_LENGTH;

    const I_assemblyBuf = file_content.slice(positionNow, positionNow + I_ASSEMBLY_LENGTH);
    header["I_assembly"] = I_assemblyBuf.readUInt32LE();
    positionNow += I_ASSEMBLY_LENGTH;

    const assembly_textBuf = file_content.slice(positionNow, positionNow + header["I_assembly"]);
    header["assembly_text"] = assembly_textBuf.toString("utf8");
    positionNow += header["I_assembly"];

    const I_textBuf = file_content.slice(positionNow, positionNow + I_TEXT_LENGTH);
    header["I_text"] = I_textBuf.readUInt32LE();
    positionNow += I_TEXT_LENGTH;

    const header_textBuf = file_content.slice(positionNow, positionNow + header["I_text"]);
    header["header_text"] = header_textBuf.toString("utf8");
    positionNow += header["I_text"];

    const n_refsBuf = file_content.slice(positionNow, positionNow + N_REFS_LENGTH);
    header["n_refs"] = n_refsBuf.readUInt16LE();
    positionNow += N_REFS_LENGTH;

    let refs = [];
    for (let i = 0; i < header["n_refs"]; i++) {
        const ref = {};
        ref["l_name"] = file_content.slice(positionNow, positionNow + L_NAME_LENGTH).readUInt32LE();
        positionNow += L_NAME_LENGTH;
        ref["ref_name"] = file_content.slice(positionNow, positionNow + ref["l_name"]).toString("utf8");
        positionNow += ref["l_name"];
        ref["ref_len"] = file_content.slice(positionNow, positionNow + REF_LEN_LENGTH).readUInt32LE();
        positionNow += REF_LEN_LENGTH;
        refs.push(ref);
    }
    header["refs"] = refs;

    return header;
}

async function queryBGZFIndex(indexData, chrRange, ref_id) {
    return queryBAIIC(chrRange, indexData, ref_id);
}

function BintoVirtualOffset(hexString, pos) {
    const chunkStartBin = hexString.substring(pos + 12, pos + 28);
    const chunkStart_block_offset = hex_to_int(chunkStartBin.substring(0, 4));
    const chunkStart_block_address = hex_to_int(chunkStartBin.substring(4, 16));
    const chunkStart = new VirtualOffset(chunkStart_block_address, chunkStart_block_offset);

    const chunkEndBin = hexString.substring(pos + 28, pos + 44);
    const chunkEnd_block_offset = hex_to_int(chunkEndBin.substring(0, 4));
    const chunkEnd_block_address = hex_to_int(chunkEndBin.substring(4, 16));
    const chunkEnd = new VirtualOffset(chunkEnd_block_address, chunkEnd_block_offset);

    return { chunkStart: chunkStart, chunkEnd: chunkEnd };
}

function queryBAIIC(chrRange, hexString, refID) {
    const startBin = reg_to_bin(chrRange.start, chrRange.start + 1);
    const endBin = reg_to_bin(chrRange.end, chrRange.end + 1);

    let startBinHex = addTrailingZeros(int_to_hex(startBin), 8);
    let endBinHex = addTrailingZeros(int_to_hex(endBin), 8);
    const refIDHex = addTrailingZeros(int_to_hex(refID), 4);

    let startPos = -1;
    while (startPos == -1) {
        startPos = hexString.indexOf(`${refIDHex}${startBinHex}`);
        startBinHex = binHexAddOne(startBinHex);
    }
    startBinHex = binHexMinusOne(startBinHex);

    let endPos = -1;
    while (endPos == -1) {
        endPos = hexString.indexOf(`${refIDHex}${endBinHex}`);
        endBinHex = binHexMinusOne(endBinHex);
    }
    endBinHex = binHexAddOne(endBinHex);
    // let testPos = hexString.indexOf(`${refIDHex}${startBinHex}`);
    let chunksPos = [];
    let chunks = [];
    // let blockAddressSet = new Set();
    for (
        let theBinHex = startBinHex;
        hex_to_int(theBinHex) <= hex_to_int(endBinHex);
        theBinHex = binHexAddOne(theBinHex)
    ) {
        const thePos = hexString.indexOf(`${refIDHex}${theBinHex}`);
        if (thePos != -1) {
            const vOff = BintoVirtualOffset(hexString, thePos);
            chunksPos.push(thePos);
            chunks.push(vOff);
            // blockAddressSet.add(vOff.blockAddress);
        }
    }
    return chunks;
}

async function queryBAllCChunks(fileHandle, chunks) {
    let mc_records_with_repeated_items = [];
    await Promise.all(
        chunks.map(async (chunk, index) => {
            const chunk_mc_records = await queryChunkNew(fileHandle, chunk);
            mc_records_with_repeated_items.push(...chunk_mc_records);
        })
    );
    return mc_records_with_repeated_items;
}

function arrangeBAllCChunks(chunks) {
    let chunkSet = new Set();
    chunks.forEach(item => {
        chunkSet.add(item['chunkStart'].blockAddress);
        chunkSet.add(item['chunkEnd'].blockAddress);
    });
    let sortedArray = Array.from(chunkSet).sort((a, b) => a - b);
    return sortedArray;
}

async function queryChunk(fileHandle, blockAddress, startOffset, endOffset) {
    // endOffset += 2 * MC_RECORD_SIZE;
    endOffset = (endOffset + 2 * MC_RECORD_SIZE > 65535) ? 65535 : endOffset + 2 * MC_RECORD_SIZE;
    const chunkBuf = Buffer.allocUnsafe(endOffset);
    const { allBytesRead } = await fileHandle.read(chunkBuf, 0, endOffset, blockAddress);
    const unzippedChunk = await unzip(chunkBuf);
    const chunk = unzippedChunk.slice(startOffset, endOffset);
    const leng_mc_cov = 2;
    let mc_records = [];
    for (let positionStartNow = 0; positionStartNow < chunk.length - MC_RECORD_SIZE; ) {
        let mc_record = {};
        mc_record["pos"] = chunk.slice(positionStartNow, positionStartNow + 4).readUInt32LE();
        positionStartNow += 4;
        mc_record["ref_id"] = chunk.slice(positionStartNow, positionStartNow + 2).readUInt16LE();
        positionStartNow += 2;
        mc_record["mc"] = chunk.slice(positionStartNow, positionStartNow + leng_mc_cov).readUInt16LE();
        positionStartNow += leng_mc_cov;
        mc_record["cov"] = chunk.slice(positionStartNow, positionStartNow + leng_mc_cov).readUInt16LE();
        positionStartNow += leng_mc_cov;
        mc_records.push(mc_record);
    }
    return mc_records;
}

async function queryChunks(fileHandle, chunks) {
    let mc_records_with_repeated_items = [];
    const blocksAddressArray = arrangeBAllCChunks(chunks);
    const startBlockOffset = chunks[0]['chunkStart'].blockOffset;
    const endBlockOffset = chunks[chunks.length - 1]['chunkEnd'].blockOffset;

    if(blocksAddressArray.length === 1){
        const mc_records_chunk = await queryChunk(
            fileHandle,
            blocksAddressArray[0],
            startBlockOffset,
            endBlockOffset
        );
        mc_records_with_repeated_items.push(...mc_records_chunk);
    } else if(blocksAddressArray.length === 2){
        const mc_records_chunk_begin = await queryChunk(
            fileHandle,
            blocksAddressArray[0],
            startBlockOffset,
            65535
        );
        mc_records_with_repeated_items.push(...mc_records_chunk_begin);

        const mc_records_chunk_end = await queryChunk(
            fileHandle,
            blocksAddressArray[1],
            endBlockOffset % MC_RECORD_SIZE,
            endBlockOffset
        );
        mc_records_with_repeated_items.push(...mc_records_chunk_end);
    } else if(blocksAddressArray.length > 2) {
        const mc_records_chunk_begin = await queryChunk(
            fileHandle,
            blocksAddressArray[0],
            startBlockOffset,
            65535
        );
        mc_records_with_repeated_items.push(...mc_records_chunk_begin);

        const chunksMiddle = blocksAddressArray.slice(1, -1);
        await Promise.all(
            chunksMiddle.map(async (chunk, index) => {
                const mc_records_chunk_middle = await queryChunk(
                    fileHandle,
                    chunk,
                    endBlockOffset % MC_RECORD_SIZE,
                    65535
                );
                mc_records_with_repeated_items.push(...mc_records_chunk_middle);
            })
        );

        const mc_records_chunk_end = await queryChunk(
            fileHandle,
            blocksAddressArray[1],
            endBlockOffset % MC_RECORD_SIZE,
            endBlockOffset
        );
        mc_records_with_repeated_items.push(...mc_records_chunk_end);
    } else {
        await Promise.all(
            chunks.map(async (chunk, index) => {
                const chunk_mc_records = await queryChunkNew(fileHandle, chunk);
                mc_records_with_repeated_items.push(...chunk_mc_records);
            })
        );
    }

    return mc_records_with_repeated_items;
}

async function queryChunkNew(fileHandle, chunk) {
    const startBlock = chunk["chunkStart"];
    const endBlock = chunk["chunkEnd"];
    let mc_records = [];
    if (startBlock["blockAddress"] == endBlock["blockAddress"]) { //Only query one block
        const mc_records_chunk = await queryChunk(
            fileHandle,
            startBlock["blockAddress"],
            startBlock["blockOffset"],
            endBlock["blockOffset"]
        );
        mc_records.push(...mc_records_chunk);
    } else {
        const chunkStart_mc_records = await queryChunk( //Query two blocks
            fileHandle,
            startBlock["blockAddress"],
            startBlock["blockOffset"],
            65535
        );
        mc_records.push(...chunkStart_mc_records);
        const chunkEnd_mc_records = await queryChunk(
            fileHandle,
            endBlock["blockAddress"],
            endBlock["blockOffset"] % MC_RECORD_SIZE,
            endBlock["blockOffset"]
        );
        mc_records.push(...chunkEnd_mc_records);
    }
    return mc_records;
}

function checkRangeRef(header, inputChrRange) {
    const ref_id = header["refs"].findIndex((dict) => dict["ref_name"] === inputChrRange.chr);
    if (ref_id === -1) {
        throw new Error("The chromosome was not found!");
    }
    const ref = header["refs"].find((item) => item.ref_name === inputChrRange.chr);
    if (inputChrRange.end > ref["ref_len"]) {
        throw new Error("The query range is outside of the reference size!");
    }
    return 0;
}

export { Czip };
