
## Setting up the project

```bash
# Clone the repo
git clone git@github.ibm.com:rakeshj/lakevision.git

```

## Developing

Once you've cloned the project, you can get to the project directory and then to the `fe` directory and install the dependencies with `npm install` (or `pnpm install` or `yarn`) start a development server:

```bash
cd lakevision/fe

npm install

```

## Start the development server
Before starting the fe server, make sure the backend fastapi server is running. Set the environment variable pointing to the backend api server (e.g. http://localhost:8000)

```bash
export PUBLIC_API_SERVER=<api_server_address_and_port>

npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

## Building

To create a production version of your app:

```bash
npm run build
```

You can preview the production build with `npm run preview`.

> To deploy your app, you may need to install an [adapter](https://kit.svelte.dev/docs/adapters) for your target environment.
