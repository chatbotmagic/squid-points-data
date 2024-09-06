import fs from 'fs'
import assert from 'assert'
import {EvmBatchProcessor} from '@subsquid/evm-processor'
import {Database, LocalDest} from '@subsquid/file-store'
import {Column, Table, Types} from '@subsquid/file-store-csv'

const height = 15740721 // 2024-09-03T23:59:59.000+0000

const processor = new EvmBatchProcessor()
	.setBlockRange({
		from: 15426752, // 2024-08-28T00:00:02.000+0000
		to: height
	})
	.setGateway('https://v2.archive.subsquid.io/network/arthera-mainnet')
	.addTransaction({})
	.setFields({
		transaction: {
			from: true,
			hash: true,
			value: true
		},
		block: {
			timestamp: true
		}
	})

const dbOptions = {
	tables: {
		TransfersTable: new Table(
			'artheraTransactions.csv',
			{
//				txnHash: Column(Types.String()),
//				timestamp: Column(Types.Numeric()),
				from: Column(Types.String()),
//				value: Column(Types.String())
			}
		)
	},
	dest: new LocalDest('./assets/artheraTransactions'),
	chunkSizeMb: Infinity
}

const excludeWalletsFilePath = './assets/excludedWallets.json'; 
const excludedWalletsArray = JSON.parse(fs.readFileSync(excludeWalletsFilePath, 'utf-8')); 
const excludedWallets = new Set(excludedWalletsArray.map((wallet: string) => wallet.toLowerCase()));

processor.run(new Database(dbOptions), async (ctx) => {
  for (let block of ctx.blocks) {
    for (let txn of block.transactions) {
      if (!excludedWallets.has(txn.from.toLocaleLowerCase())) {
		ctx.store.TransfersTable.write({
			//				txnHash: txn.hash,
			//				timestamp: block.header.timestamp,
							from: txn.from,
			//				value: txn.value.toString()
						})
      }
    }
  }

  if (ctx.blocks[ctx.blocks.length - 1].header.height >= height) {
    ctx.log.info(`Last processed block was at ${new Date(ctx.blocks[ctx.blocks.length - 1].header.timestamp)}`);
    ctx.store.setForceFlush(true);
  }
});

