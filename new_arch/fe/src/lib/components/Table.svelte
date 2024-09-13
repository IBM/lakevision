<script>
// @ts-nocheck

	import { Loading } from 'carbon-components-svelte';
	import Maximize from 'carbon-icons-svelte/lib/Maximize.svelte';
	import Minimize from 'carbon-icons-svelte/lib/Minimize.svelte';
	import Misuse from "carbon-icons-svelte/lib/Misuse.svelte";

	import { onMount } from 'svelte';
	/**
	 * @type {any[]}
	 */
	 export let fetched_data;
	export let loading;
	let error = '';
	/**
	 * @type {string | null}
	 */
	let sortedColumn = null;
	let sortDirection = 'asc';

	/**
	 * @param {string} column
	 */
	function sortTable(column) {
		// Toggle the sort direction
		if (sortedColumn === column) {
			sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
		} else {
			sortedColumn = column;
			sortDirection = 'asc';
		}
		// Sort the data
		fetched_data = [...fetched_data].sort((a, b) => {
			let aValue = a[column];
			let bValue = b[column];

			if (typeof aValue === 'string') {
				aValue = aValue.toLowerCase();
				bValue = bValue.toLowerCase();
			}

			if (aValue < bValue) {
				return sortDirection === 'asc' ? -1 : 1;
			}
			if (aValue > bValue) {
				return sortDirection === 'asc' ? 1 : -1;
			}
			return 0;
		});
	}

	let isFullScreen = false;

	// Toggle full screen mode
	function toggleFullScreen() {
		isFullScreen = !isFullScreen;
	}
	let showPopover = false;
	let popoverContent = '';
	let popoverPosition = { top: 0, left: 0 };

	// Show popover on double-click
	/**
	 * @param {MouseEvent & { currentTarget: EventTarget & HTMLDivElement; }} event
	 * @param {string} content
	 */
	function handleDoubleClick(event, content) {
		popoverContent = content;
		popoverPosition = { top: event.clientY, left: event.clientX };
		showPopover = true;
	}

	function closePopover() {
		showPopover = false;
	}
</script>

<section class="bx--table-toolbar">
	<div class="bx--toolbar-content">
		<div class="table-wrapper">
			{#if fetched_data}
				<button class="bx--btn" on:click={toggleFullScreen}>
					{#if isFullScreen}<Minimize size={16} />{:else}<Maximize size={16} />{/if}
				</button>
			{/if}
		</div>
	</div>
</section>
<div>
	<div
		class="bx--data-table-container {isFullScreen ? 'fullscreen' : ''}"
		style="overflow: auto; max-width: 100%; {!isFullScreen ? 'max-height:400px' : ''}"
	>
		{#if loading}
			<p align="center">
				<br />
				<Loading withOverlay={false} small />
				<br />
			</p>
		{:else if error}
			<p>Error: {error}</p>
		{:else if fetched_data && fetched_data[0]}
			{#if isFullScreen}
				<div class="bx--toolbar-content">
					<button class="bx--btn" on:click={toggleFullScreen}>
						{#if isFullScreen}<Minimize size={20} />{:else}<Maximize size={16} />{/if}
					</button>
				</div>
			{/if}
			<table class="bx--data-table">
				<thead>
					<tr>
						{#each Object.keys(fetched_data[0]) as key}
							<th class="bx--table-header" on:click={() => sortTable(key)}>
								{key}
								{#if sortedColumn === key}
									{#if sortDirection === 'asc'}
										▲
									{:else}
										▼
									{/if}
								{/if}
							</th>
						{/each}
					</tr>
				</thead>

				<tbody>
					{#each fetched_data as row}
						<tr style="max-height: 50px; overflow: hidden;">
							{#each Object.values(row) as value}
								<td class="bx--table-column truncate">
									{#if Array.isArray(value)}
										<ul>
											{#each value as item}
												{#if typeof item === 'object' && item !== null}
													<li>
														{item}
													</li>
												{:else}
													<li><div>{item}</div></li>
												{/if}
											{/each}
										</ul>
									{:else if typeof value === 'object' && value !== null}
										{#each Object.entries(value) as [k, v]}
											<ul>
												{k}={v}
											</ul>
										{/each}
									{:else}
										<div
											role="button"
											tabindex="0"
											on:dblclick={(e) => handleDoubleClick(e, value)}
										>
											{value}
										</div>
									{/if}
								</td>
							{/each}
						</tr>
					{/each}
				</tbody>
			</table>
		{:else}
			No data
		{/if}
	</div>
</div>
<!-- Popover Component -->
{#if showPopover}
	<div
		aria-hidden="true"
		class="popover"
		style="top: {popoverPosition.top}px; left: {popoverPosition.left}px;"
		on:click={closePopover}
	>
		<div role="button" class="popover-content">
			<div aria-hidden="true" class="bx--btn" on:click={closePopover} style="fixed: left;"><Misuse size={20}/></div>			
			<div class="popover-text">
				{popoverContent}
			</div>
		</div>
	</div>
{/if}

<style>
	table {
		/* Make sure the table takes full width */
		width: 100%;
		border-collapse: collapse;
	}

	th,
	td {
		/* Styling for table cells */
		padding: 8px;
		border: 1px solid #ddd;
		text-align: left;
	}

	th {
		background-color: #f2f2f2;
	}
	/* Full-screen styling */
	.fullscreen {
		position: fixed;
		top: 0;
		left: 0;
		width: 100vw;
		height: 100vh;
		z-index: 9999;
		background-color: white; /* Optional: Set a background color */
		max-height: 100vh;
		max-width: 100vw;
		overflow: auto;
	}

	.bx--btn {
		margin-bottom: 1rem;
	}

	/* Popover Styling */
	.popover {
		position: absolute;
		z-index: 1100;
		background-color: white;
		border: 1px solid #0d060661;
		border-radius: 8px;
		box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
		padding: 1rem;
		max-width: 600px;
		overflow: hidden;
	}

	.popover-content {
		max-height: 200px; /* Set max height for scrolling */
		overflow-y: auto;
	}

	.popover-text {
		white-space: pre-wrap;
		word-wrap: break-word;
	}

	.table-wrapper {
		position: relative;
		overflow-x: auto; /* Allows horizontal scroll */
	}

	/* Truncate text in cells */
	.truncate {
		max-width: 200px; /* Restrict the width of the cell */
		white-space: nowrap; /* Prevent text from wrapping to a new line */
		overflow: hidden; /* Hide any overflowing text */
		text-overflow: ellipsis; /* Add ellipsis (...) to indicate truncated text */
	}
</style>
