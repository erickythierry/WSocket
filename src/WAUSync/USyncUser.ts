import { BinaryNode } from '../WABinary'

export class USyncUser {
	id: string
	lid: string
	phone: string
	type: string
	personaId: string
	/** nós extras dentro do <user>, ex.: <tctoken> pra ler status de contato restrito */
	content?: BinaryNode[]

	withId(id: string) {
		this.id = id
		return this
	}

	withLid(lid: string) {
		this.lid = lid
		return this
	}

	withPhone(phone: string) {
		this.phone = phone
		return this
	}

	withType(type: string) {
		this.type = type
		return this
	}

	withPersonaId(personaId: string) {
		this.personaId = personaId
		return this
	}

	withContent(content: BinaryNode[]) {
		this.content = content
		return this
	}
}
