fn main() {
    #[cfg(windows)]
    {
        let mut resource = winresource::WindowsResource::new();
        resource.set_icon("icon.ico");
        resource
            .compile()
            .expect("failed to embed Windows icon resource");
    }
}
