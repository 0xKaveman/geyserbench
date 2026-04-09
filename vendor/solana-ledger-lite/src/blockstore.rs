use solana_clock::Slot;

pub(crate) fn verify_shred_slots(slot: Slot, parent: Slot, root: Slot) -> bool {
    if slot == 0 && parent == 0 && root == 0 {
        return true;
    }

    root <= parent && parent < slot
}
