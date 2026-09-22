"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeNewsletterSocket = void 0;
const Types_1 = require("../Types");
const messages_media_1 = require("../Utils/messages-media");
const WABinary_1 = require("../WABinary");
const groups_1 = require("./groups");
const mex_1 = require("./mex");
const parseNewsletterCreateResponse = (response) => {
    const { id, thread_metadata: thread, viewer_metadata: viewer } = response;
    return {
        id: id,
        owner: undefined,
        name: thread.name.text,
        creation_time: parseInt(thread.creation_time, 10),
        description: thread.description.text,
        invite: thread.invite,
        subscribers: parseInt(thread.subscribers_count, 10),
        verification: thread.verification,
        picture: {
            id: thread.picture.id,
            directPath: thread.picture.direct_path
        },
        mute_state: viewer.mute
    };
};
const parseNewsletterMetadata = (result) => {
    if (typeof result !== 'object' || result === null) {
        return null;
    }
    if ('id' in result && typeof result.id === 'string') {
        const raw = result;
        const thread = typeof raw.thread_metadata === 'object' && raw.thread_metadata !== null
            ? raw.thread_metadata
            : undefined;
        const viewer = typeof raw.viewer_metadata === 'object' && raw.viewer_metadata !== null
            ? raw.viewer_metadata
            : undefined;
        const textValue = (value) => {
            if (typeof value === 'string')
                return value;
            if (typeof value === 'object' && value !== null && typeof value.text === 'string') {
                return value.text;
            }
        };
        const numberValue = (value) => {
            const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
            return Number.isFinite(parsed) ? parsed : undefined;
        };
        const pictureRaw = typeof (thread === null || thread === void 0 ? void 0 : thread.picture) === 'object' && thread.picture !== null
            ? thread.picture
            : undefined;
        return {
            ...raw,
            id: result.id,
            name: textValue(raw.name) || textValue(thread === null || thread === void 0 ? void 0 : thread.name) || '',
            description: textValue(raw.description) || textValue(thread === null || thread === void 0 ? void 0 : thread.description),
            invite: typeof raw.invite === 'string' ? raw.invite : typeof (thread === null || thread === void 0 ? void 0 : thread.invite) === 'string' ? thread.invite : undefined,
            creation_time: numberValue(raw.creation_time) || numberValue(thread === null || thread === void 0 ? void 0 : thread.creation_time),
            subscribers: numberValue(raw.subscribers) || numberValue(thread === null || thread === void 0 ? void 0 : thread.subscribers_count),
            picture: pictureRaw
                ? {
                    id: typeof pictureRaw.id === 'string' ? pictureRaw.id : undefined,
                    directPath: typeof pictureRaw.direct_path === 'string'
                        ? pictureRaw.direct_path
                        : typeof pictureRaw.directPath === 'string'
                            ? pictureRaw.directPath
                            : undefined
                }
                : raw.picture,
            verification: raw.verification === 'VERIFIED' || raw.verification === 'UNVERIFIED'
                ? raw.verification
                : (thread === null || thread === void 0 ? void 0 : thread.verification) === 'VERIFIED' || (thread === null || thread === void 0 ? void 0 : thread.verification) === 'UNVERIFIED'
                    ? thread.verification
                    : undefined,
            mute_state: raw.mute_state === 'ON' || raw.mute_state === 'OFF'
                ? raw.mute_state
                : (viewer === null || viewer === void 0 ? void 0 : viewer.mute) === 'ON' || (viewer === null || viewer === void 0 ? void 0 : viewer.mute) === 'OFF'
                    ? viewer.mute
                    : undefined
        };
    }
    if ('result' in result && typeof result.result === 'object' && result.result !== null && 'id' in result.result) {
        return parseNewsletterMetadata(result.result);
    }
    return null;
};
const makeNewsletterSocket = (config) => {
    const sock = (0, groups_1.makeGroupsSocket)(config);
    const { query, generateMessageTag } = sock;
    const executeWMexQuery = (variables, queryId, dataPath) => {
        return (0, mex_1.executeWMexQuery)(variables, queryId, dataPath, query, generateMessageTag);
    };
    const newsletterUpdate = async (jid, updates) => {
        const variables = {
            newsletter_id: jid,
            updates: {
                ...updates,
                settings: null
            }
        };
        return executeWMexQuery(variables, Types_1.QueryIds.UPDATE_METADATA, 'xwa2_newsletter_update');
    };
    return {
        ...sock,
        newsletterCreate: async (name, description) => {
            const variables = {
                input: {
                    name,
                    description: description !== null && description !== void 0 ? description : null
                }
            };
            const rawResponse = await executeWMexQuery(variables, Types_1.QueryIds.CREATE, Types_1.XWAPaths.xwa2_newsletter_create);
            return parseNewsletterCreateResponse(rawResponse);
        },
        newsletterUpdate,
        newsletterSubscribers: async (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.SUBSCRIBERS, Types_1.XWAPaths.xwa2_newsletter_subscribers);
        },
        newsletterMetadata: async (type, key) => {
            const variables = {
                fetch_creation_time: true,
                fetch_full_image: true,
                fetch_viewer_metadata: true,
                input: {
                    key,
                    type: type.toUpperCase()
                }
            };
            const result = await executeWMexQuery(variables, Types_1.QueryIds.METADATA, Types_1.XWAPaths.xwa2_newsletter_metadata);
            return parseNewsletterMetadata(result);
        },
        /** Retorna os canais que a conta segue ou administra. */
        newsletterSubscribed: async () => {
            const result = await executeWMexQuery({}, Types_1.QueryIds.SUBSCRIBED, Types_1.XWAPaths.xwa2_newsletter_subscribed);
            return result.map(parseNewsletterMetadata).filter((item) => item !== null);
        },
        newsletterFollow: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.FOLLOW, Types_1.XWAPaths.xwa2_newsletter_follow);
        },
        newsletterUnfollow: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.UNFOLLOW, Types_1.XWAPaths.xwa2_newsletter_unfollow);
        },
        newsletterMute: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.MUTE, Types_1.XWAPaths.xwa2_newsletter_mute_v2);
        },
        newsletterUnmute: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.UNMUTE, Types_1.XWAPaths.xwa2_newsletter_unmute_v2);
        },
        newsletterUpdateName: async (jid, name) => {
            return await newsletterUpdate(jid, { name });
        },
        newsletterUpdateDescription: async (jid, description) => {
            return await newsletterUpdate(jid, { description });
        },
        newsletterUpdatePicture: async (jid, content) => {
            const { img } = await (0, messages_media_1.generateProfilePicture)(content);
            return await newsletterUpdate(jid, { picture: img.toString('base64') });
        },
        newsletterRemovePicture: async (jid) => {
            return await newsletterUpdate(jid, { picture: '' });
        },
        newsletterReactMessage: async (jid, serverId, reaction) => {
            await query({
                tag: 'message',
                attrs: {
                    to: jid,
                    ...(reaction ? {} : { edit: '7' }),
                    type: 'reaction',
                    server_id: serverId,
                    id: generateMessageTag()
                },
                content: [
                    {
                        tag: 'reaction',
                        attrs: reaction ? { code: reaction } : {}
                    }
                ]
            });
        },
        newsletterFetchMessages: async (jid, count, since, after) => {
            const messageUpdateAttrs = {
                count: count.toString()
            };
            if (typeof since === 'number') {
                messageUpdateAttrs.since = since.toString();
            }
            if (after) {
                messageUpdateAttrs.after = after.toString();
            }
            const result = await query({
                tag: 'iq',
                attrs: {
                    id: generateMessageTag(),
                    type: 'get',
                    xmlns: 'newsletter',
                    to: jid
                },
                content: [
                    {
                        tag: 'message_updates',
                        attrs: messageUpdateAttrs
                    }
                ]
            });
            return result;
        },
        subscribeNewsletterUpdates: async (jid) => {
            var _a;
            const result = await query({
                tag: 'iq',
                attrs: {
                    id: generateMessageTag(),
                    type: 'set',
                    xmlns: 'newsletter',
                    to: jid
                },
                content: [{ tag: 'live_updates', attrs: {}, content: [] }]
            });
            const liveUpdatesNode = (0, WABinary_1.getBinaryNodeChild)(result, 'live_updates');
            const duration = (_a = liveUpdatesNode === null || liveUpdatesNode === void 0 ? void 0 : liveUpdatesNode.attrs) === null || _a === void 0 ? void 0 : _a.duration;
            return duration ? { duration: duration } : null;
        },
        newsletterAdminCount: async (jid) => {
            const response = await executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.ADMIN_COUNT, Types_1.XWAPaths.xwa2_newsletter_admin_count);
            return response.admin_count;
        },
        newsletterChangeOwner: async (jid, newOwnerJid) => {
            await executeWMexQuery({ newsletter_id: jid, user_id: newOwnerJid }, Types_1.QueryIds.CHANGE_OWNER, Types_1.XWAPaths.xwa2_newsletter_change_owner);
        },
        newsletterDemote: async (jid, userJid) => {
            await executeWMexQuery({ newsletter_id: jid, user_id: userJid }, Types_1.QueryIds.DEMOTE, Types_1.XWAPaths.xwa2_newsletter_demote);
        },
        newsletterDelete: async (jid) => {
            await executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.DELETE, Types_1.XWAPaths.xwa2_newsletter_delete_v2);
        }
    };
};
exports.makeNewsletterSocket = makeNewsletterSocket;
