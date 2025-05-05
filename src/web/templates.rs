use minijinja::Environment;
use std::collections::HashMap;
use tracing::error;

#[allow(unused)]
pub fn init_templates() -> Environment<'static> {
    let mut env = Environment::new();

    // Register built-in templates
    env.add_template("error.html", include_str!("../../templates/error.html"))
        .expect("Failed to add error template");

    // Add filters
    env.add_filter("json", |value: minijinja::value::Value| {
        serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())
    });

    env
}

#[allow(unused)]
pub fn render_template(
    env: &Environment,
    template_name: &str,
    context: HashMap<&str, minijinja::value::Value>,
) -> String {
    match env.get_template(template_name) {
        Ok(tmpl) => match tmpl.render(context) {
            Ok(result) => result,
            Err(e) => {
                error!("Template render error: {}", e);
                format!("<h1>Template Error</h1><p>{}</p>", e)
            }
        },
        Err(e) => {
            error!("Template not found: {} ({})", template_name, e);
            format!("<h1>Template Not Found</h1><p>{}: {}</p>", template_name, e)
        }
    }
}