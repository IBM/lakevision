<script>
    import CloseOutline from "carbon-icons-svelte/lib/CloseOutline.svelte";
	export let showModal; // boolean

	let dialog; // HTMLDialogElement

	$: if (dialog && showModal) dialog.showModal();

    import { createEventDispatcher } from 'svelte';

    const dispatch = createEventDispatcher();
    
    function handleModalClose() {
        showModal = false;
        dialog.close();
        dispatch('modelClosed');
    }

</script>

<!-- svelte-ignore a11y-click-events-have-key-events a11y-no-noninteractive-element-interactions -->
<dialog class="bx--modal-container" 
	bind:this={dialog}
	on:close={() => (handleModalClose())}
	on:click|self={() => handleModalClose()}
>
	<!-- svelte-ignore a11y-no-static-element-interactions -->
	<div on:click|stopPropagation class="bx--modal-content">
		<slot name="header" />
        <!-- svelte-ignore a11y-autofocus -->
        <div align="right" on:click={() => handleModalClose()}>
            <CloseOutline size={20} />            
        </div>
		<hr />
		<slot />			
	</div>
</dialog>

<style>    
	dialog {
		max-width: 36em;
		border-radius: 0.1em;
		border: none;
		padding: 0;
	}
	dialog::backdrop {
		background: rgba(0, 0, 0, 0.3);
	}
	dialog > div {
		padding: 1em;
	}
	dialog[open] {
		animation: zoom 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
	}
	@keyframes zoom {
		from {
			transform: scale(0.95);
		}
		to {
			transform: scale(1);
		}
	}
	dialog[open]::backdrop {
		animation: fade 0.1s ease-out;
	}
	@keyframes fade {
		from {
			opacity: 0;
		}
		to {
			opacity: 1;
		}
	}	
</style>