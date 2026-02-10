use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Style, Stylize},
    text::Line,
    widgets::{Block, BorderType, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

pub const BORDER_TYPE: BorderType = BorderType::Rounded;
pub const HORIZONTAL: &str = BORDER_TYPE.to_border_set().horizontal_top;

pub fn styled_block<'a>(
    title: &'a str,
    focused: bool,
    bottom_title: Option<Line<'a>>,
) -> Block<'a> {
    let mut title_text = title.white();
    let mut border_color = Style::new().white();
    if focused {
        title_text = title_text.bold();
        border_color = border_color.green();
    }

    let title_line = Line::from(vec![HORIZONTAL.into(), title_text]);
    let mut block = Block::bordered()
        .border_type(BORDER_TYPE)
        .border_style(border_color)
        .title_top(title_line);

    if let Some(bt) = bottom_title {
        block = block.title_bottom(bt.right_aligned());
    }

    block
}

pub fn render_horizontal_scrollbar(
    frame: &mut Frame,
    area: Rect,
    content_len: usize,
    position: usize,
    right_margin: u16,
) {
    let left_margin = 1;
    let scrollbar_area = Rect {
        x: area.x + left_margin,
        y: area.y,
        width: area
            .width
            .saturating_sub(right_margin)
            .saturating_sub(left_margin),
        height: area.height,
    };

    let mut state = ScrollbarState::default()
        .content_length(content_len)
        .position(position);

    let scrollbar = Scrollbar::new(ScrollbarOrientation::HorizontalBottom)
        .thumb_symbol("\u{1FB0B}")
        .track_symbol(None)
        .begin_symbol(None)
        .end_symbol(None);

    frame.render_stateful_widget(scrollbar, scrollbar_area, &mut state);
}

pub fn render_vertical_scrollbar(
    frame: &mut Frame,
    area: Rect,
    content_len: usize,
    position: usize,
) {
    let mut state = ScrollbarState::default()
        .content_length(content_len)
        .position(position);

    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .thumb_symbol("\u{2590}")
        .track_symbol(None)
        .begin_symbol(None)
        .end_symbol(None);

    frame.render_stateful_widget(
        scrollbar,
        area.inner(Margin {
            vertical: 1,
            horizontal: 0,
        }),
        &mut state,
    );
}
