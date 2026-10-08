import { USyncQueryProtocol } from '../../Types/USync'
import { BinaryNode, getBinaryNodeChild } from '../../WABinary'
//import { USyncUser } from '../USyncUser'

export type KeyIndexData = {
	timestamp: number
	signedKeyIndex?: Uint8Array
	expectedTimestamp?: number
}

export type DeviceListData = {
	id: number
	keyIndex?: number
	isHosted?: boolean
}

export type ParsedDeviceInfo = {
	deviceList?: DeviceListData[]
	keyIndex?: KeyIndexData
	/** o servidor respondeu <error> para este usuário: lista vazia não quer dizer "sem device" */
	error?: boolean
}

export class USyncDeviceProtocol implements USyncQueryProtocol {
	name = 'devices'

	getQueryElement(): BinaryNode {
		return {
			tag: 'devices',
			attrs: {
				version: '2'
			}
		}
	}

	getUserElement(/* user: USyncUser */): BinaryNode | null {
		//TODO: Implement device phashing, ts and expectedTs
		//TODO: if all are not present, return null <- current behavior
		//TODO: otherwise return a node w tag 'devices' w those as attrs
		return null
	}

	parser(node: BinaryNode): ParsedDeviceInfo {
		const deviceList: DeviceListData[] = []
		let keyIndex: KeyIndexData | undefined = undefined

		// usuário com <error> volta sem devices: fica fora do envio em vez de abortar o grupo inteiro
		if (node.tag === 'devices' && getBinaryNodeChild(node, 'error')) {
			return { deviceList, error: true }
		}

		if (node.tag === 'devices') {
			const deviceListNode = getBinaryNodeChild(node, 'device-list')
			const keyIndexNode = getBinaryNodeChild(node, 'key-index-list')

			if (Array.isArray(deviceListNode?.content)) {
				for (const { tag, attrs } of deviceListNode.content) {
					const id = +attrs.id
					const keyIndex = +attrs['key-index']
					if (tag === 'device') {
						deviceList.push({
							id,
							keyIndex,
							isHosted: !!(attrs['is_hosted'] && attrs['is_hosted'] === 'true')
						})
					}
				}
			}

			if (keyIndexNode?.tag === 'key-index-list') {
				keyIndex = {
					timestamp: +keyIndexNode.attrs['ts'],
					signedKeyIndex: keyIndexNode?.content as Uint8Array,
					expectedTimestamp: keyIndexNode.attrs['expected_ts'] ? +keyIndexNode.attrs['expected_ts'] : undefined
				}
			}
		}

		return {
			deviceList,
			keyIndex
		}
	}
}
