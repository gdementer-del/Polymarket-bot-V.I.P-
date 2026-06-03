//! Shared label normalization for mixed English/Russian Polymarket exports.

fn normalized(value: &str) -> String {
    value.trim().to_lowercase()
}

pub(super) fn outcome_label_is_up(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "up" | "yes"
            | "y"
            | "\u{440}\u{43e}\u{441}\u{442}"
            | "\u{432}\u{432}\u{435}\u{440}\u{445}"
            | "\u{434}\u{430}"
    )
}

pub(super) fn outcome_label_is_down(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "down"
            | "no"
            | "n"
            | "\u{43f}\u{430}\u{434}\u{435}\u{43d}\u{438}\u{435}"
            | "\u{432}\u{43d}\u{438}\u{437}"
            | "\u{43d}\u{435}\u{442}"
    )
}

pub(super) fn outcome_label_is_flat(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "flat" | "\u{444}\u{43b}\u{44d}\u{442}" | "\u{444}\u{43b}\u{435}\u{442}"
    )
}

pub(super) fn wallet_side_is_buy_label(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "buy"
            | "bought"
            | "purchase"
            | "\u{43f}\u{43e}\u{43a}\u{443}\u{43f}\u{43a}\u{430}"
            | "\u{43a}\u{443}\u{43f}\u{438}\u{442}\u{44c}"
            | "\u{43a}\u{443}\u{43f}\u{43b}\u{435}\u{43d}\u{43e}"
    )
}

pub(super) fn wallet_side_is_sell_label(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "sell"
            | "sold"
            | "sale"
            | "\u{43f}\u{440}\u{43e}\u{434}\u{430}\u{436}\u{430}"
            | "\u{43f}\u{440}\u{43e}\u{434}\u{430}\u{442}\u{44c}"
            | "\u{43f}\u{440}\u{43e}\u{434}\u{430}\u{43d}\u{43e}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_labels_accept_english_and_russian_forms() {
        assert!(outcome_label_is_up("Up"));
        assert!(outcome_label_is_up("\u{420}\u{43e}\u{441}\u{442}"));
        assert!(outcome_label_is_down("Down"));
        assert!(outcome_label_is_down(
            "\u{41f}\u{430}\u{434}\u{435}\u{43d}\u{438}\u{435}"
        ));
        assert!(outcome_label_is_flat("\u{424}\u{43b}\u{44d}\u{442}"));
    }

    #[test]
    fn wallet_side_labels_accept_english_and_russian_forms() {
        assert!(wallet_side_is_buy_label("Buy"));
        assert!(wallet_side_is_buy_label(
            "\u{41f}\u{43e}\u{43a}\u{443}\u{43f}\u{43a}\u{430}"
        ));
        assert!(wallet_side_is_sell_label("Sell"));
        assert!(wallet_side_is_sell_label(
            "\u{41f}\u{440}\u{43e}\u{434}\u{430}\u{436}\u{430}"
        ));
    }
}
