import { BinaryNode } from '../WABinary';
/**
 * Builds the `ack` stanza to be sent back for a received node.
 *
 * Sanitizes the attribute set: only a fixed allow-list of attrs from the
 * incoming node is ever echoed back. Preserves the fork-specific behavior:
 *  - `type` is omitted on successful `message` acks (unless the incoming node
 *    carries an `unavailable` child, in which case `type` is echoed and `from`
 *    is injected with the local user id).
 *  - `type` is always echoed for non-message stanzas (receipt, notification,
 *    iq, call, ...) when present.
 *
 * Returns `undefined` when the incoming node does not carry the attributes
 * (`id`, `from`) required to build a valid ack — the caller is expected to
 * skip the send in that case.
 */
export declare const buildAckStanza: ({ tag, attrs, content }: BinaryNode, errorCode: number | undefined, meId: string | undefined) => BinaryNode | undefined;
