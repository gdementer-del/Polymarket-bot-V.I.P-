//! Shared label normalization for mixed English/Russian Polymarket exports.

fn normalized(value: &str) -> String {
    value.trim().to_lowercase()
}

pub(super) fn outcome_label_is_up(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "up" | "yes" | "y" | "рост" | "вверх" | "да"
    )
}

pub(super) fn outcome_label_is_down(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "down" | "no" | "n" | "падение" | "вниз" | "нет"
    )
}

pub(super) fn outcome_label_is_flat(value: &str) -> bool {
    matches!(normalized(value).as_str(), "flat" | "флэт" | "флет")
}

pub(super) fn wallet_side_is_buy_label(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "buy" | "bought" | "purchase" | "покупка" | "купить" | "куплено"
    )
}

pub(super) fn wallet_side_is_sell_label(value: &str) -> bool {
    matches!(
        normalized(value).as_str(),
        "sell" | "sold" | "sale" | "продажа" | "продать" | "продано"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_labels_accept_english_and_russian_forms() {
        assert!(outcome_label_is_up("Up"));
        assert!(outcome_label_is_up("Рост"));
        assert!(outcome_label_is_down("Down"));
        assert!(outcome_label_is_down("Падение"));
        assert!(outcome_label_is_flat("Флэт"));
    }

    #[test]
    fn wallet_side_labels_accept_english_and_russian_forms() {
        assert!(wallet_side_is_buy_label("Buy"));
        assert!(wallet_side_is_buy_label("Покупка"));
        assert!(wallet_side_is_sell_label("Sell"));
        assert!(wallet_side_is_sell_label("Продажа"));
    }
}
