<script>
	import { env } from '$env/dynamic/public';
    import 'carbon-components-svelte/css/all.css';
	import {
		Header,
        HeaderUtilities,
        HeaderActionLink,
		ComboBox,		
		SideNav,
		SideNavItems,		
		SkipToContent
	} from 'carbon-components-svelte';
	import LogoGithub from "carbon-icons-svelte/lib/LogoGithub.svelte";
	import { selectedNamespce } from '$lib/stores';
	import { selectedTable } from '$lib/stores';
	//import { onMount } from 'svelte';

	export let data;
	let isSideNavOpen = false;
	
	/**
	 * @type {never[]}
	 */
	let tables = [];
	let loading = false;

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
            
			const res = await fetch(`${env.PUBLIC_API_SERVER}/tables?namespace=${namespace}`);
			console.log(res.ok);
			if (res.ok) {
				tables = await res.json();
			} else {
				console.error('Failed to fetch data:', res.statusText);
			}
		} finally {
			loading = false; // Stop loading indicator
		}
	}

	function shouldFilterItem(item, value) {
		if (!value) return true;
		return item.text.toLowerCase().includes(value.toLowerCase());
	}

	let dropdown1_selectedId = '';
	let dropdown2_selectedId = '';

	const formatSelected = (id, items) => items.find((item) => item.id === id)?.text ?? '';

	$: ns = get_tables(formatSelected(dropdown1_selectedId, data.namespaces));
	$: selectedTable.set(formatSelected(dropdown2_selectedId, tables));
</script>

<Header company="Apache Iceberg" platformName="Lakevision" bind:isSideNavOpen>
	<svelte:fragment slot="skip-to-content">
		<SkipToContent />
	</svelte:fragment>
    <HeaderUtilities>
        <HeaderActionLink
            href="https://github.com/IBM/lakevision"
            target="_blank">
            <LogoGithub slot="icon" size={20} />
        </HeaderActionLink>
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
		/>

		<br /> <br /><br /> <br /><br /> <br />

		<ComboBox
			titleText="Table"
			disabled={loading}
			items={tables}
			bind:selectedId={dropdown2_selectedId}
			{shouldFilterItem}
		/>
	</SideNavItems>
</SideNav>

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
</style>
