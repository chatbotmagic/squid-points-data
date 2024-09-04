import axios from 'axios'
import fs from 'fs'

async function main() {
	let env = ''

	const ethereumHeight = (await axios.get('https://v2.archive.subsquid.io/network/ethereum-mainnet/height')).data
	env += `ETH_ARCHIVE_HEIGHT=${ethereumHeight}\n`

	const arbitrumHeight = (await axios.get('https://v2.archive.subsquid.io/network/arbitrum-one/height')).data
	env += `ARBITRUM_ONE_ARCHIVE_HEIGHT=${arbitrumHeight}\n`

	const bscHeight = (await axios.get('https://v2.archive.subsquid.io/network/binance-mainnet/height')).data
	env += `BINANCE_ARCHIVE_HEIGHT=${bscHeight}\n`

	const neonHeight = (await axios.get('https://v2.archive.subsquid.io/network/neon-mainnet/height')).data
	env += `NEON_ARCHIVE_HEIGHT=${neonHeight}\n`

	const coreHeight = (await axios.get('https://v2.archive.subsquid.io/network/core-mainnet/height')).data
	env += `CORE_ARCHIVE_HEIGHT=${coreHeight}\n`

	const merlinHeight = (await axios.get('https://v2.archive.subsquid.io/network/merlin-mainnet/height')).data
	env += `MERLIN_ARCHIVE_HEIGHT=${merlinHeight}\n`

	const optimismHeight = (await axios.get('https://v2.archive.subsquid.io/network/optimism-mainnet/height')).data
	env += `OPTIMISM_ARCHIVE_HEIGHT=${optimismHeight}\n`

	const artheraHeight = (await axios.get('https://v2.archive.subsquid.io/network/arthera-mainnet/height')).data
	env += `ARTHERA_ARCHIVE_HEIGHT=${artheraHeight}\n`

	await fs.writeFile('.env', env, e => { if (e) console.error(e); else console.log('.env written successfully') })
}

main()
