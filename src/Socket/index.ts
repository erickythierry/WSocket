import { DEFAULT_CONNECTION_CONFIG } from '../Defaults'
import { UserFacingSocketConfig } from '../Types'
import { makeBusinessSocket } from './business'

// export the last socket layer
const makeWASocket = (config: UserFacingSocketConfig) => {
	const sock = makeBusinessSocket({
		...DEFAULT_CONNECTION_CONFIG,
		...config
	})
	// cada camada espalha a de baixo e congela o getter; `user` volta a refletir o creds atual (lid, nome)
	Object.defineProperty(sock, 'user', {
		get: () => sock.authState.creds.me,
		enumerable: true,
		configurable: true
	})
	return sock
}

export default makeWASocket
