"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Defaults_1 = require("../Defaults");
const business_1 = require("./business");
// export the last socket layer
const makeWASocket = (config) => {
    const sock = (0, business_1.makeBusinessSocket)({
        ...Defaults_1.DEFAULT_CONNECTION_CONFIG,
        ...config
    });
    // cada camada espalha a de baixo e congela o getter; `user` volta a refletir o creds atual (lid, nome)
    Object.defineProperty(sock, 'user', {
        get: () => sock.authState.creds.me,
        enumerable: true,
        configurable: true
    });
    return sock;
};
exports.default = makeWASocket;
