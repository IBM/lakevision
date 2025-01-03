<script>
	import { createVirtualizer } from '@tanstack/svelte-virtual';
    import { onMount } from 'svelte';
    import { page } from '$app/stores';

	export let data = []; // Array of row objects
	export let columns = []; // Array of columns
	export let rowHeight = 40; // Height of each row
    export let tableHeight = 500;
    export let defaultColumnWidth = 200;

	let containerRef;

	// Sorting state
	let sortKey = null;
	let sortOrder = 'asc';

	// Popover state
	let showPopover = false;
	let popoverContent = '';
	let popoverPosition = { top: 0, left: 0 };
    let columnWidths = {};  
    let startX, startWidth, columnKey;

    onMount(() => {
        const storedWidths = localStorage.getItem('columnWidths');
        if (storedWidths) {
        columnWidths = JSON.parse(storedWidths);
        }
    });

    //in case columnWidths need to be reset from localstorage
    let reset_cw = $page.url.searchParams.get('reset_cw');

	// Sorted data
	$: sortedData = [...data].sort((a, b) => {
		if (!sortKey) return 0;
		if (a[sortKey] < b[sortKey]) return sortOrder === 'asc' ? -1 : 1;
		if (a[sortKey] > b[sortKey]) return sortOrder === 'asc' ? 1 : -1;
		return 0;
	});

	// Virtualizer instance
	$: rowVirtualizer = createVirtualizer({
		count: sortedData.length,
		getScrollElement: () => containerRef,
		estimateSize: () => rowHeight
	});

	function handleSort(columnKey) {
		if (sortKey === columnKey) {
			sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
		} else {
			sortKey = columnKey;
			sortOrder = 'asc';
		}
	}

	function handleDoubleClick(event, content) {
		popoverContent = content;
		showPopover = true;

		const rect = event.target.getBoundingClientRect();
		popoverPosition = {
			top: rect.top + rect.height + window.scrollY,
			left: rect.left + window.scrollX
		};
	}

	function closePopover() {
		showPopover = false;
		popoverContent = '';
	}

    function formatValue(value) {
        //return JSON.stringify(value);
        if (Array.isArray(value)) {
            return value.join(' \n ');
        } else if (typeof value === 'object' && value !== null) {        
            return Object.entries(value)
                .map(([k, v]) => `${k}: ${v}`)
                .join(', \n');
        }        
        return value;
    }
        // Handle the start of the drag event (mouse down)
    function handleMouseDown(event, key) {
        startX = event.clientX;
        startWidth = columnWidths[key] || 200; // Default width is 200px if no custom width
        columnKey = key;

        // Add event listeners for mouse movement and mouse up
        document.addEventListener('mousemove', handleMouseMove);
        document.addEventListener('mouseup', handleMouseUp);
    }

    // Handle the mouse movement during dragging
    function handleMouseMove(event) {
        const newWidth = startWidth + (event.clientX - startX);
        columnWidths[columnKey] = Math.max(newWidth, 50); // Set a minimum width of 50px
        saveColumnWidths();  // Save the column widths after every change
    }

    // Handle the mouse up event (end of dragging)
    function handleMouseUp() {
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
    }

    function saveColumnWidths() {
        localStorage.setItem('columnWidths', JSON.stringify(columnWidths));
    }

    function resetColumnWidths() {
        columnWidths = Object.keys(columns).reduce((acc, key) => {
            acc[key] = defaultColumnWidth;
            return acc;
            }, {});
        localStorage.removeItem('columnWidths');
        return '';
    }
</script>
{#if reset_cw }
    {resetColumnWidths()}
{/if}
<div bind:this={containerRef} class="table-container" style="height: {tableHeight}px">
	<!-- Sticky Header -->
	<div class="sticky-header">
		{#each Object.keys(columns) as key}
			<div
				class="header-cell"
                style="width: {columnWidths[key] || defaultColumnWidth}px"
				title={key}
				on:click={() => handleSort(key)}
				role="button"
				tabindex="0"
				on:keypress={(e) => {
					if (e.key === 'Enter' || e.key === ' ') handleSort(key);
				}}
			>
				{key}
				{#if sortKey === key}
					{sortOrder === 'asc' ? ' ▲' : ' ▼'}
				{/if}
                <div
                    class="resize-handle"
                    on:mousedown={(event) => handleMouseDown(event, key)} >
                </div>
			</div>
		{/each}
	</div>

	<!-- Virtualized Rows -->
	<div style="position: relative; height: {$rowVirtualizer.getTotalSize()}px;">
		{#each $rowVirtualizer.getVirtualItems() as virtualRow}
			<div
				class="row"
				style="transform: translateY({virtualRow.start}px); height: {virtualRow.size}px;"
			>
				{#each Object.keys(columns) as key}
					<div
						role="button"
						tabindex="0"
						on:keypress={(e) => {
							if (e.key === 'Enter' || e.key === ' ')
								handleDoubleClick(event, sortedData[virtualRow.index]?.[key]);
						}}
						class="cell"
						on:dblclick={(event) => handleDoubleClick(event, sortedData[virtualRow.index]?.[key])}                        
                        style="width: {columnWidths[key] || defaultColumnWidth}px"
					>
						{formatValue(sortedData[virtualRow.index]?.[key])}
					</div>
				{/each}
			</div>
		{/each}
	</div>    
</div>

{#if showPopover}
	<div class="popover" style="top: {popoverPosition.top}px; left: {popoverPosition.left}px;">
		<div class="popover-close" role="button"
        tabindex="0"
        on:keypress={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {closePopover};
        }} on:click={closePopover}>✖</div>
		<br />
		
        <pre style="white-space: pre-wrap;">{formatValue(popoverContent)}</pre>
	</div>
{/if}

<style>
	.table-container {
		
		overflow-y: auto;
		overflow-x: auto;
		position: relative;
		border: 1px solid #ccc;
	}

	.sticky-header {
		position: sticky;
		top: 0;
		background-color: #f4f4f4;
		z-index: 2;
		display: flex;
		width: fit-content;
	}

	.header-cell,
	.cell {
        position: relative;
		padding: 8px;
		border: 1px solid #ddd;
		text-align: left;
		white-space: nowrap;
		width: 200px;
		overflow: hidden;
		text-overflow: ellipsis;
		box-sizing: border-box;
	}
    .resize-handle {
        position: absolute;
        right: 0;
        top: 0;
        width: 5px;
        height: 100%;
        cursor: ew-resize; /* Horizontal resize cursor */
        background-color: transparent;
    }
	.row {
		display: flex;
		position: absolute;
		width: fit-content;
	}

	.popover {
		position: absolute;
		z-index: 10;
		background: white;
		border: 1px solid #0e0e0e;
		box-shadow: 0px 2px 8px rgba(0, 0, 0, 0.1);
		padding: 8px;
		border-radius: 4px;
		max-width: 400px;
		max-height: 450px; /* Set a fixed height */
		overflow-y: auto; /* Enable vertical scrolling */
		word-wrap: break-word;
		white-space: pre-wrap;
	}

	.popover-close {
		position: absolute;
		top: 5px;
		left: 5px; /* Move it to the left */
		cursor: pointer;
		font-size: 12px;
		color: #555;
		background: #f4f4f4;
		border: none;
		padding: 2px 5px;
		border-radius: 3px;
		box-shadow: 0px 1px 3px rgba(0, 0, 0, 0.1);
	}
</style>
