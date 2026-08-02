import { BinaryNode } from '../WABinary';
export declare class USyncUser {
    id: string;
    lid: string;
    phone: string;
    type: string;
    personaId: string;
    /** nós extras dentro do <user>, ex.: <tctoken> pra ler status de contato restrito */
    content?: BinaryNode[];
    withId(id: string): this;
    withLid(lid: string): this;
    withPhone(phone: string): this;
    withType(type: string): this;
    withPersonaId(personaId: string): this;
    withContent(content: BinaryNode[]): this;
}
