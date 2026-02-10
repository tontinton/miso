pub struct TextBuffer {
    lines: Vec<String>,
    raw_x: usize,
    cursor_y: usize,
    scroll_x: usize,
    width: usize,
    multiline: bool,
}

impl TextBuffer {
    pub fn new(input: String, multiline: bool) -> Self {
        let lines = if multiline {
            input.split('\n').map(str::to_string).collect()
        } else {
            vec![input]
        };
        Self {
            lines,
            raw_x: 0,
            cursor_y: 0,
            scroll_x: 0,
            width: 0,
            multiline,
        }
    }

    pub fn single_line() -> Self {
        Self::new(String::new(), false)
    }

    pub fn value(&self) -> String {
        self.lines.join("\n")
    }

    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    pub fn first_line(&self) -> &str {
        &self.lines[0]
    }

    pub fn x(&self) -> usize {
        self.raw_x.min(self.current_line_width())
    }

    pub fn y(&self) -> usize {
        self.cursor_y
    }

    pub fn scroll_x(&self) -> usize {
        self.scroll_x
    }

    pub fn set_width(&mut self, width: usize) {
        self.width = width;
    }

    pub fn line_count(&self) -> usize {
        self.lines.len()
    }

    pub fn max_line_width(&self) -> usize {
        self.lines.iter().map(|l| l.len()).max().unwrap_or(0)
    }

    fn current_line_width(&self) -> usize {
        self.lines[self.cursor_y].len()
    }

    pub fn push_char(&mut self, c: char) {
        let x = self.x();
        self.lines[self.cursor_y].insert(x, c);
        self.raw_x = x + 1;
        self.adjust_scroll();
    }

    pub fn add_line(&mut self) {
        if !self.multiline {
            return;
        }
        let x = self.x();
        let (left, right) = self.lines[self.cursor_y].split_at(x);
        let (left, right) = (left.to_string(), right.to_string());
        self.lines[self.cursor_y] = left;
        self.lines.insert(self.cursor_y + 1, right);
        self.raw_x = 0;
        self.cursor_y += 1;
        self.adjust_scroll();
    }

    pub fn remove_char(&mut self) {
        let x = self.x();
        if x == 0 {
            self.merge_with_previous_line();
        } else {
            self.lines[self.cursor_y].remove(x - 1);
            self.raw_x = x - 1;
        }
        self.adjust_scroll();
    }

    pub fn delete_char(&mut self) {
        let x = self.x();
        if x == self.current_line_width() {
            if self.multiline && self.cursor_y + 1 < self.lines.len() {
                let next_line = self.lines.remove(self.cursor_y + 1);
                self.lines[self.cursor_y].push_str(&next_line);
            }
        } else {
            self.lines[self.cursor_y].remove(x);
        }
    }

    pub fn remove_line(&mut self) {
        if !self.multiline {
            self.lines[0].clear();
            self.raw_x = 0;
            return;
        }
        if self.lines.len() == 1 {
            self.lines[0].clear();
        } else {
            self.lines.remove(self.cursor_y);
        }
        if self.cursor_y >= self.lines.len() {
            self.cursor_y = self.lines.len() - 1;
        }
        self.raw_x = self.current_line_width();
        self.adjust_scroll();
    }

    pub fn remove_word_before_cursor(&mut self) {
        let x = self.x();
        if x == 0 {
            self.merge_with_previous_line();
            return;
        }

        let mut new_x = x;
        let line = &self.lines[self.cursor_y];
        while new_x > 0 && line.as_bytes()[new_x - 1].is_ascii_whitespace() {
            new_x -= 1;
        }
        while new_x > 0 && !line.as_bytes()[new_x - 1].is_ascii_whitespace() {
            new_x -= 1;
        }

        self.lines[self.cursor_y].replace_range(new_x..x, "");
        self.raw_x = new_x;
        self.adjust_scroll();
    }

    pub fn move_left(&mut self) {
        let x = self.x();
        if x > 0 {
            self.raw_x = x - 1;
        } else if self.multiline && self.cursor_y > 0 {
            self.cursor_y -= 1;
            self.raw_x = self.lines[self.cursor_y].len();
        }
        self.adjust_scroll();
    }

    pub fn move_right(&mut self) {
        let x = self.x();
        if x < self.current_line_width() {
            self.raw_x = x + 1;
        } else if self.multiline && self.cursor_y < self.lines.len() - 1 {
            self.raw_x = 0;
            self.cursor_y += 1;
        }
        self.adjust_scroll();
    }

    pub fn move_up(&mut self) {
        if self.multiline && self.cursor_y > 0 {
            self.cursor_y -= 1;
        }
    }

    pub fn move_down(&mut self) {
        if self.multiline && self.cursor_y < self.lines.len() - 1 {
            self.cursor_y += 1;
        }
    }

    pub fn move_home(&mut self) {
        self.raw_x = 0;
        self.adjust_scroll();
    }

    pub fn move_end(&mut self) {
        self.raw_x = self.current_line_width();
        self.adjust_scroll();
    }

    fn merge_with_previous_line(&mut self) {
        if !self.multiline || self.cursor_y == 0 {
            return;
        }
        self.raw_x = self.lines[self.cursor_y - 1].len();
        let line = self.lines.remove(self.cursor_y);
        self.lines[self.cursor_y - 1].push_str(&line);
        self.cursor_y -= 1;
    }

    fn adjust_scroll(&mut self) {
        if self.width == 0 {
            return;
        }
        let x = self.x();
        if x < self.scroll_x {
            self.scroll_x = x.saturating_sub(self.width);
        } else if x >= self.scroll_x + self.width {
            self.scroll_x = x - self.width + 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TextBuffer;

    #[test]
    fn single_line_editing() {
        let mut buf = TextBuffer::single_line();
        buf.push_char('a');
        buf.push_char('c');
        buf.raw_x = 1;
        buf.push_char('b');
        assert_eq!(buf.value(), "abc");

        buf.remove_char();
        assert_eq!(buf.value(), "ac");

        buf.add_line();
        assert_eq!(buf.line_count(), 1);
    }

    #[test]
    fn multiline_line_splitting_and_merging() {
        let mut buf = TextBuffer::new("abcd".into(), true);
        buf.raw_x = 2;
        buf.add_line();
        assert_eq!(buf.lines(), &["ab", "cd"]);
        assert_eq!((buf.y(), buf.x()), (1, 0));

        // backspace at start of line merges with previous
        buf.remove_char();
        assert_eq!(buf.value(), "abcd");
        assert_eq!((buf.y(), buf.x()), (0, 2));

        // delete at end of line merges with next
        buf.raw_x = 4;
        buf.add_line();
        buf.push_char('e');
        buf.cursor_y = 0;
        buf.raw_x = 4;
        buf.delete_char();
        assert_eq!(buf.value(), "abcde");
        assert_eq!(buf.line_count(), 1);
    }

    #[test]
    fn remove_line() {
        let mut buf = TextBuffer::new("only".into(), true);
        buf.remove_line();
        assert_eq!(buf.value(), "");
        assert_eq!(buf.line_count(), 1);
    }

    #[test]
    fn cursor_wraps_across_lines() {
        let mut buf = TextBuffer::new("ab\ncd".into(), true);
        buf.raw_x = 2;
        buf.move_right();
        assert_eq!((buf.y(), buf.x()), (1, 0));

        buf.move_left();
        assert_eq!((buf.y(), buf.x()), (0, 2));
    }

    #[test]
    fn sticky_x_preserved_across_short_lines() {
        let mut buf = TextBuffer::new("long\nhi\nlong".into(), true);
        buf.raw_x = 4;
        buf.move_down();
        assert_eq!(buf.x(), 2);
        buf.move_down();
        assert_eq!(buf.x(), 4);
    }

    #[test]
    fn remove_word_before_cursor() {
        let mut buf = TextBuffer::new("hello world".into(), true);
        buf.raw_x = 11;
        buf.remove_word_before_cursor();
        assert_eq!(buf.value(), "hello ");

        buf.remove_word_before_cursor();
        assert_eq!(buf.value(), "");
    }

    #[test]
    fn scroll_adjusts_when_cursor_exceeds_width() {
        let mut buf = TextBuffer::new("abcdefgh".into(), true);
        buf.set_width(4);
        buf.raw_x = 8;
        buf.adjust_scroll();
        assert!(buf.scroll_x > 0);
        assert!(buf.x() < buf.scroll_x + buf.width);
    }
}
