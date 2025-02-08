<script>
	import { env } from '$env/dynamic/public';
    import 'carbon-components-svelte/css/all.css';
	import {
		Header,
        HeaderUtilities,
        HeaderActionLink,
		HeaderGlobalAction,
		ComboBox,		
		SideNav,
		SideNavItems,		
		SkipToContent,
		SideNavLink,
		Modal
		
	} from 'carbon-components-svelte';	
	import LogoGithub from "carbon-icons-svelte/lib/LogoGithub.svelte";
	import { selectedNamespce } from '$lib/stores';
	import { selectedTable } from '$lib/stores';
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { goto } from "$app/navigation";
	import { Logout, UserAvatarFilledAlt } from 'carbon-icons-svelte';

	let dropdown1_selectedId = '';
	let dropdown2_selectedId = '';
	let namespace;
	
  	// Extract query parameters
  	let q_ns = $page.url.searchParams.get('namespace');
  	let q_tab = $page.url.searchParams.get('table');

	export let data;
	let isSideNavOpen = true;
	let user;
	let AUTH_ENABLED = false;
	/**
	 * @type {never[]}
	 */
	let tables = [];
	let loading = false;
	const tableLoadedEvent = new EventTarget();

	/**
	 * @param {null} namespace
	 */
	async function get_tables(namespace) {
		loading = true;
		if (!namespace) {
			loading = false;
			// @ts-ignore
			selectedNamespce.set("");
            // @ts-ignore
            selectedTable.set("");
            dropdown2_selectedId = '';
			return;
		}
		try {
			selectedNamespce.set(namespace);
            dropdown2_selectedId = '';
            
			const res = await fetch(`/api/tables?namespace=${namespace}`);
			console.log(res.ok);
			if (res.ok) {
				tables = await res.json();
			} else {
				console.error('Failed to fetch data:', res.statusText);
			}
		} finally {
			loading = false; // Stop loading indicator
			tableLoadedEvent.dispatchEvent(new Event("tablesLoaded"));
		}
	}
	
	function waitForTables() {
		return new Promise((resolve) => {
			tableLoadedEvent.addEventListener("tablesLoaded", () => {
			resolve(tables); // Resolve when the event is triggered
			});
		});
	}

	function shouldFilterItem(item, value) {
		if (!value) return true;
		return item.text.toLowerCase().includes(value.toLowerCase());
	}

	const formatSelected = (id, items) => items.find((item) => item.id === id)?.text ?? '';

	function findItemIdByText(items, text) {
		const item = items.find(item => item.text === text);
		return item ? item.id : null;
	}

	function resetQueryParams() {
		const url = window.location.origin + window.location.pathname;	
		window.history.replaceState(null, '', url);
	}

	$: ns = get_tables(formatSelected(dropdown1_selectedId, data.namespaces));
	$: selectedTable.set(formatSelected(dropdown2_selectedId, tables));

	if(q_ns){
		setNamespace(q_ns);			
	}

	onMount(() => {		
		if(env.PUBLIC_AUTH_ENABLED=='true'){AUTH_ENABLED=true;}
		if(AUTH_ENABLED && user==null){
			const params = new URLSearchParams(window.location.search);
			const code = params.get("code");
			if (code) {
				exchangeCodeForToken(code);		
			} else {
				console.error("No authorization code found!");
				login();
			}
		}

		if(q_tab){
			waitForTables().then((result) => {
				console.log("The value is available:", result); 
				const id = findItemIdByText(tables, q_tab);			
				console.log(id);		
				dropdown2_selectedId = id;	
				resetQueryParams();
			});
		}		
  	});

	function setNamespace(nsp){
		const id = findItemIdByText(data.namespaces, nsp);
		dropdown1_selectedId = id;	
		navpop = false;
	}
	function setTable(tab){
		const id = findItemIdByText(tables, tab);
		dropdown2_selectedId = id;	
		tabpop = false;
	}
	$: {
        selectedNamespce.subscribe(value => {namespace = value; });
	}
	let navpop = false;
	let tabpop = false;

	async function exchangeCodeForToken(code) {
		const response = await fetch("/api/auth/token", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ code }),
		credentials: "include", 
		});

		if (response.ok) {
			const data = await response.json();
			console.log("Token received:", data);
			user = data;
			goto("/"); 
		} else {
			console.error("Error exchanging token");
		}
	}

	const clientId 			= env.PUBLIC_OPENID_CLIENT_ID;
  	const openidProviderUrl = env.PUBLIC_OPENID_PROVIDER_URL+"/authorize";
  	const redirectUri 		= encodeURIComponent(env.PUBLIC_REDIRECT_URI);

	function login() {
		window.location.href = `${openidProviderUrl}?client_id=${clientId}&redirect_uri=${redirectUri}&response_type=code&scope=openid%20profile%20email`;
		showLogin = false;
	}

	async function logout() {
		const res = await fetch('/api/logout');		
		user = ''
		return;
	}

	let showLogin = false;	
	let loginPosition = { top: 0, left: 0 };

	function handleLogout(event) {		
		logout();
		showLogin = true;
		const rect = event.target.getBoundingClientRect();
		loginPosition = {
			top: rect.top + rect.height + window.scrollY,
			left: rect.left + window.scrollX
		};		
	}

</script>

<Header company="Apache Iceberg" platformName="Lakevision" >
	<svelte:fragment slot="skip-to-content">
		<SkipToContent />
	</svelte:fragment>
    <HeaderUtilities>
		<HeaderActionLink
            href="https://github.com/IBM/lakevision"
            target="_blank">
            <LogoGithub slot="icon" size={20} />
        </HeaderActionLink>
		{#if AUTH_ENABLED}
			<HeaderGlobalAction iconDescription={user}  icon={UserAvatarFilledAlt}/>		
			<HeaderGlobalAction iconDescription='Logout'  icon={Logout} on:click={(event) => handleLogout(event)}/>
		{/if}
    </HeaderUtilities>
</Header>
<SideNav bind:isOpen={isSideNavOpen}>
	<SideNavItems>
		<br />		
		<ComboBox
			titleText="Namespace"
			items={data.namespaces}						
			bind:selectedId={dropdown1_selectedId}
			{shouldFilterItem}
			let:item
		>
		<div>
			<strong>{item.text}</strong>
		</div>		
	</ComboBox>
	<br />
	<SideNavLink on:click={() => ( navpop=true)}>Show All</SideNavLink>
	
		<br /> <br /><br /> <br /><br /> <br />
		<ComboBox
			titleText="Table"
			disabled={loading}
			items={tables}
			bind:selectedId={dropdown2_selectedId}
			{shouldFilterItem}
			let:item
			>
			<div>
				<strong>{item.text}</strong>
			</div>		
		</ComboBox>
		<br />
		<SideNavLink on:click={() => ( tabpop=true)}>Show All</SideNavLink>
	</SideNavItems>
</SideNav>
{#if navpop}
	<Modal size="sm" passiveModal bind:open={navpop} modalHeading="Namespaces" on:open on:close>
		<table>
			{#each data.namespaces as ns}                
			<tr>
				<td>{ns.id}</td> <td><div 
					role="button"
					tabindex="0" 
					on:keypress={(e) => {
						if (e.key === 'Enter' || e.key === ' ') setNamespace(ns.text);
					}}
					on:click={setNamespace(ns.text)}><a href={'#'}> {ns.text}</a></div></td>		  	
			</tr>
		{/each}            
		</table>
	</Modal>
{/if}
{#if tabpop}
<!--<Modal showModal={navpop} on:modelClosed={()=>(navpop=false)} >  {showNamespaes()}  </Modal> -->
	<Modal size="sm" passiveModal bind:open={tabpop} modalHeading={"Tables in "+namespace} on:open on:close>
		<table>
			{#each tables as tabs}                
			<tr>
				<td>{tabs.id}</td> <td><div 
					role="button"
					tabindex="0" 
					on:keypress={(e) => {
						if (e.key === 'Enter' || e.key === ' ') setTable(tabs.text);
					}}
					on:click={setTable(tabs.text)}> <a href={'#'}>{tabs.text}</a></div></td>		  	
			</tr>
		{/each}            
		</table>
	</Modal>
{/if}
<Modal
  bind:open={showLogin}
  modalHeading="Login"
  primaryButtonText="Login"
  secondaryButtonText="Cancel"
  on:click:button--secondary={() => (showLogin = false)}  
  on:open
  on:close
  on:submit={login}
/>
<slot></slot>

<style>
	/* Adjust the content container to account for the wider side nav */
	:global(.bx--content) {
		margin-left: 300px; /* Match the SideNav width */
	}
	/* Adjust the content container to account for the wider side nav */
	:global(.bx--label) {
		margin-left: 15px; /* Match the SideNav width */
        color: #fff;
		font-weight: bold;
	}
    :global(.bx--side-nav__navigation) {
    background-color: #161616; 
  }

  :global(.bx--side-nav__link .bx--side-nav__icon svg) {
    fill: #fff; 
  }
  
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 0px;
    }
  
    td {
      border: 0.25px solid #ccc;
      padding: 8px;
      text-align: left;
    }  
  
</style>
