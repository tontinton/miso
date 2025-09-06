use ratatui::widgets::BorderType;

pub const BORDER_TYPE: BorderType = BorderType::Rounded;
pub const HORIZONTAL: &str = BORDER_TYPE.to_border_set().horizontal_top;
