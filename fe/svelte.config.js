//import adapter from '@sveltejs/adapter-auto';
import { optimizeImports } from 'carbon-preprocess-svelte';
import adapter from '@sveltejs/adapter-node';
/** @type {import('@sveltejs/kit').Config} */
const config = {
	preprocess: [
		// Optimize Carbon imports for faster development and build times.
		optimizeImports()
	],
	kit: {
		// adapter-auto only supports some environments, see https://kit.svelte.dev/docs/adapter-auto for a list.
		// If your environment is not supported, or you settled on a specific environment, switch out the adapter.
		// See https://kit.svelte.dev/docs/adapters for more information about adapters.
		adapter: adapter()
	}
};

export default config;
