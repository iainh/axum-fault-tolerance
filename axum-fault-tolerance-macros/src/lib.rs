use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Expr, FnArg, GenericArgument, ImplItem, ImplItemFn, ItemImpl, Lit, LitFloat, LitInt,
    LitStr, Pat, PathArguments, Receiver, ReturnType, Type, parse_macro_input,
};

/// Rewrites annotated methods in an inherent `impl` block to use
/// `axum-fault-tolerance` runtime policies.
///
/// Supported method attributes are `#[retry]`, `#[timeout]`, `#[fallback]`,
/// `#[circuit_breaker]`, and `#[bulkhead]`.
#[proc_macro_attribute]
pub fn fault_tolerant(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut impl_item = parse_macro_input!(item as ItemImpl);
    TokenStream::from(expand_fault_tolerant(&mut impl_item))
}

fn expand_fault_tolerant(impl_item: &mut ItemImpl) -> TokenStream2 {
    let ft = quote!(::axum_fault_tolerance);
    let mut errors = Vec::new();
    let mut items = Vec::new();
    let mut has_annotated_method = false;

    if impl_item.trait_.is_some() {
        errors.push(
            syn::Error::new_spanned(
                &impl_item.self_ty,
                "#[fault_tolerant] must be used on an inherent impl block",
            )
            .to_compile_error(),
        );
    }

    for item in std::mem::take(&mut impl_item.items) {
        let ImplItem::Fn(mut method) = item else {
            items.push(item);
            continue;
        };

        let had_policy_attrs = method.attrs.iter().any(is_policy_attr);
        match take_policy_attrs(&mut method.attrs) {
            Ok(policy) if !policy.is_enabled() => {
                items.push(ImplItem::Fn(method));
            }
            Ok(policy) => {
                has_annotated_method = true;
                match expand_method(&ft, method, policy) {
                    Ok(expanded) => items.extend(expanded),
                    Err(error) => errors.push(error.to_compile_error()),
                }
            }
            Err(error) => {
                has_annotated_method |= had_policy_attrs;
                errors.push(error.to_compile_error());
                items.push(ImplItem::Fn(method));
            }
        }
    }

    impl_item.items = items;

    if !has_annotated_method {
        errors.push(
            syn::Error::new_spanned(
                &impl_item.self_ty,
                "#[fault_tolerant] requires at least one method annotated with a fault tolerance policy",
            )
            .to_compile_error(),
        );
    }

    quote! {
        #impl_item
        #(#errors)*
    }
}

fn expand_method(
    ft: &TokenStream2,
    mut method: ImplItemFn,
    policy: PolicyAttrs,
) -> syn::Result<Vec<ImplItem>> {
    validate_method(&method)?;

    let original_ident = method.sig.ident.clone();
    let hidden_ident = format_ident!("__fault_tolerance_{original_ident}");
    let arg_idents = argument_idents(&method)?;
    let original_vis = method.vis.clone();
    let original_attrs = method.attrs.clone();

    method.sig.ident = hidden_ident.clone();
    method.vis = syn::Visibility::Inherited;
    method.attrs.clear();

    let policy_builder = policy.builder_tokens(ft);
    let operation = quote! {
        || {
            #(let #arg_idents = #arg_idents.clone();)*
            async move { self.#hidden_ident(#(#arg_idents),*).await }
        }
    };
    let has_fallback = policy.fallback_method.is_some();
    let call = if let Some(fallback_method) = &policy.fallback_method {
        quote! {
            policy
                .call_with_fallback(
                    #operation,
                    |error| async move {
                        #(let #arg_idents = #arg_idents.clone();)*
                        self.#fallback_method(#(#arg_idents,)* error).await
                    },
                )
                .await
        }
    } else {
        quote! {
            policy.call(#operation).await
        }
    };

    let mut wrapper = method.clone();
    wrapper.sig.ident = original_ident;
    wrapper.vis = original_vis;
    wrapper.attrs = original_attrs;
    if !has_fallback {
        wrapper.sig.output = fault_tolerance_output(ft, &wrapper.sig.output)?;
    }
    wrapper.block = syn::parse_quote!({
        let policy = #policy_builder;
        #call
    });

    Ok(vec![ImplItem::Fn(method), ImplItem::Fn(wrapper)])
}

fn validate_method(method: &ImplItemFn) -> syn::Result<()> {
    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "fault tolerant methods must be async",
        ));
    }

    if method.sig.generics.lt_token.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig.generics,
            "fault tolerant methods must not have generic parameters",
        ));
    }

    match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) if is_shared_receiver(receiver) => {}
        Some(input) => {
            return Err(syn::Error::new_spanned(
                input,
                "fault tolerant methods must take &self as the first parameter",
            ));
        }
        None => {
            return Err(syn::Error::new_spanned(
                &method.sig.ident,
                "fault tolerant methods must take &self",
            ));
        }
    }

    argument_idents(method)?;
    Ok(())
}

fn is_shared_receiver(receiver: &Receiver) -> bool {
    receiver.reference.is_some() && receiver.mutability.is_none()
}

fn argument_idents(method: &ImplItemFn) -> syn::Result<Vec<syn::Ident>> {
    method
        .sig
        .inputs
        .iter()
        .skip(1)
        .map(|input| match input {
            FnArg::Typed(input) => match &*input.pat {
                Pat::Ident(ident) if ident.by_ref.is_none() && ident.mutability.is_none() => {
                    Ok(ident.ident.clone())
                }
                _ => Err(syn::Error::new_spanned(
                    &input.pat,
                    "fault tolerant method arguments must use simple identifiers",
                )),
            },
            FnArg::Receiver(_) => unreachable!("receiver was skipped"),
        })
        .collect()
}

fn fault_tolerance_output(ft: &TokenStream2, output: &ReturnType) -> syn::Result<ReturnType> {
    let ReturnType::Type(arrow, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "fault tolerant methods must return Result<T, E>",
        ));
    };

    let (ok, error) = result_types(ty)?;
    let output = if let Some(error) = error {
        syn::parse_quote!(#arrow #ft::Result<#ok, #error>)
    } else {
        syn::parse_quote!(#arrow #ft::Result<#ok>)
    };

    Ok(output)
}

fn result_types(ty: &Type) -> syn::Result<(Type, Option<Type>)> {
    let Type::Path(type_path) = ty else {
        return Err(syn::Error::new_spanned(ty, "expected Result<T, E>"));
    };

    let Some(segment) = type_path.path.segments.last() else {
        return Err(syn::Error::new_spanned(ty, "expected Result<T, E>"));
    };

    if segment.ident != "Result" {
        return Err(syn::Error::new_spanned(ty, "expected Result<T, E>"));
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(syn::Error::new_spanned(ty, "expected Result<T, E>"));
    };

    let mut type_args = args.args.iter().filter_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    });

    let Some(ok) = type_args.next() else {
        return Err(syn::Error::new_spanned(ty, "expected Result<T, E>"));
    };
    let error = type_args.next();

    Ok((ok, error))
}

#[derive(Debug, Default)]
struct PolicyAttrs {
    retry: Option<RetryAttr>,
    timeout_ms: Option<u64>,
    fallback_method: Option<syn::Ident>,
    circuit_breaker: Option<CircuitBreakerAttr>,
    bulkhead: Option<BulkheadAttr>,
}

impl PolicyAttrs {
    fn is_enabled(&self) -> bool {
        self.retry.is_some()
            || self.timeout_ms.is_some()
            || self.fallback_method.is_some()
            || self.circuit_breaker.is_some()
            || self.bulkhead.is_some()
    }

    fn builder_tokens(&self, ft: &TokenStream2) -> TokenStream2 {
        let mut setup = TokenStream2::new();
        let mut tokens = quote!(#ft::FaultTolerance::builder());

        if let Some(timeout_ms) = self.timeout_ms {
            tokens = quote! {
                #tokens.timeout(::std::time::Duration::from_millis(#timeout_ms))
            };
        }

        if let Some(retry) = &self.retry {
            let retry_tokens = retry.tokens(ft);
            tokens = quote! {
                #tokens.retry(#retry_tokens)
            };
        }

        if let Some(circuit_breaker) = &self.circuit_breaker {
            let circuit_config = circuit_breaker.config_tokens(ft);
            setup.extend(quote! {
                static CIRCUIT_BREAKER: ::std::sync::OnceLock<#ft::CircuitBreaker> = ::std::sync::OnceLock::new();
                let circuit_breaker = CIRCUIT_BREAKER
                    .get_or_init(|| #ft::CircuitBreaker::new(#circuit_config))
                    .clone();
            });
            tokens = quote! {
                #tokens.circuit_breaker(circuit_breaker)
            };
        }

        if let Some(bulkhead) = &self.bulkhead {
            let max_concurrent = bulkhead.max_concurrent;
            setup.extend(quote! {
                static BULKHEAD: ::std::sync::OnceLock<#ft::Bulkhead> = ::std::sync::OnceLock::new();
                let bulkhead = BULKHEAD
                    .get_or_init(|| #ft::Bulkhead::new(#max_concurrent as usize))
                    .clone();
            });
            tokens = quote! {
                #tokens.bulkhead(bulkhead)
            };
        }

        quote!({
            #setup
            #tokens.build()
        })
    }
}

#[derive(Debug)]
struct RetryAttr {
    max_retries: Option<u64>,
    delay_ms: Option<u64>,
    max_duration_ms: Option<u64>,
}

impl RetryAttr {
    fn tokens(&self, ft: &TokenStream2) -> TokenStream2 {
        let mut tokens = quote!(#ft::RetryPolicy::new());

        if let Some(max_retries) = self.max_retries {
            tokens = quote!(#tokens.max_retries(#max_retries as usize));
        }

        if let Some(delay_ms) = self.delay_ms {
            tokens = quote!(#tokens.delay(::std::time::Duration::from_millis(#delay_ms)));
        }

        if let Some(max_duration_ms) = self.max_duration_ms {
            tokens =
                quote!(#tokens.max_duration(::std::time::Duration::from_millis(#max_duration_ms)));
        }

        tokens
    }
}

#[derive(Debug)]
struct CircuitBreakerAttr {
    request_volume_threshold: Option<u64>,
    failure_ratio: Option<f64>,
    delay_ms: Option<u64>,
}

impl CircuitBreakerAttr {
    fn config_tokens(&self, ft: &TokenStream2) -> TokenStream2 {
        let mut config = quote!(#ft::CircuitBreakerConfig::new());

        if let Some(request_volume_threshold) = self.request_volume_threshold {
            config = quote!(#config.request_volume_threshold(#request_volume_threshold as usize));
        }

        if let Some(failure_ratio) = self.failure_ratio {
            config = quote!(#config.failure_ratio(#failure_ratio));
        }

        if let Some(delay_ms) = self.delay_ms {
            config = quote!(#config.delay(::std::time::Duration::from_millis(#delay_ms)));
        }

        config
    }
}

#[derive(Debug)]
struct BulkheadAttr {
    max_concurrent: u64,
}

fn is_policy_attr(attr: &Attribute) -> bool {
    attr.path().is_ident("retry")
        || attr.path().is_ident("timeout")
        || attr.path().is_ident("fallback")
        || attr.path().is_ident("circuit_breaker")
        || attr.path().is_ident("bulkhead")
}

fn take_policy_attrs(attrs: &mut Vec<Attribute>) -> syn::Result<PolicyAttrs> {
    let mut policy = PolicyAttrs::default();
    let mut retained = Vec::new();

    for attr in std::mem::take(attrs) {
        if attr.path().is_ident("retry") {
            reject_duplicate(policy.retry.is_some(), &attr, "retry")?;
            policy.retry = Some(parse_retry_attr(&attr)?);
        } else if attr.path().is_ident("timeout") {
            reject_duplicate(policy.timeout_ms.is_some(), &attr, "timeout")?;
            policy.timeout_ms = Some(parse_timeout_attr(&attr)?);
        } else if attr.path().is_ident("fallback") {
            reject_duplicate(policy.fallback_method.is_some(), &attr, "fallback")?;
            policy.fallback_method = Some(parse_fallback_attr(&attr)?);
        } else if attr.path().is_ident("circuit_breaker") {
            reject_duplicate(policy.circuit_breaker.is_some(), &attr, "circuit_breaker")?;
            policy.circuit_breaker = Some(parse_circuit_breaker_attr(&attr)?);
        } else if attr.path().is_ident("bulkhead") {
            reject_duplicate(policy.bulkhead.is_some(), &attr, "bulkhead")?;
            policy.bulkhead = Some(parse_bulkhead_attr(&attr)?);
        } else {
            retained.push(attr);
        }
    }

    *attrs = retained;
    Ok(policy)
}

fn parse_retry_attr(attr: &Attribute) -> syn::Result<RetryAttr> {
    let mut retry = RetryAttr {
        max_retries: None,
        delay_ms: None,
        max_duration_ms: None,
    };

    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("max_retries") {
            retry.max_retries = Some(parse_u64(&meta.value()?.parse()?)?);
        } else if meta.path.is_ident("delay_ms") {
            retry.delay_ms = Some(parse_u64(&meta.value()?.parse()?)?);
        } else if meta.path.is_ident("max_duration_ms") {
            retry.max_duration_ms = Some(parse_u64(&meta.value()?.parse()?)?);
        } else {
            return Err(meta.error("expected `max_retries`, `delay_ms`, or `max_duration_ms`"));
        }
        Ok(())
    })?;

    Ok(retry)
}

fn parse_timeout_attr(attr: &Attribute) -> syn::Result<u64> {
    let mut timeout_ms = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("ms") {
            timeout_ms = Some(parse_u64(&meta.value()?.parse()?)?);
        } else {
            return Err(meta.error("expected `ms`"));
        }
        Ok(())
    })?;
    timeout_ms.ok_or_else(|| syn::Error::new_spanned(attr, "`#[timeout]` requires `ms = ...`"))
}

fn parse_fallback_attr(attr: &Attribute) -> syn::Result<syn::Ident> {
    let mut method = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("method") {
            let method_lit: LitStr = meta.value()?.parse()?;
            method = Some(syn::Ident::new(&method_lit.value(), method_lit.span()));
        } else {
            return Err(meta.error("expected `method`"));
        }
        Ok(())
    })?;
    method.ok_or_else(|| syn::Error::new_spanned(attr, "`#[fallback]` requires `method = \"...\"`"))
}

fn parse_circuit_breaker_attr(attr: &Attribute) -> syn::Result<CircuitBreakerAttr> {
    let mut circuit_breaker = CircuitBreakerAttr {
        request_volume_threshold: None,
        failure_ratio: None,
        delay_ms: None,
    };

    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("request_volume_threshold") {
            circuit_breaker.request_volume_threshold = Some(parse_u64(&meta.value()?.parse()?)?);
        } else if meta.path.is_ident("failure_ratio") {
            circuit_breaker.failure_ratio = Some(parse_f64(&meta.value()?.parse()?)?);
        } else if meta.path.is_ident("delay_ms") {
            circuit_breaker.delay_ms = Some(parse_u64(&meta.value()?.parse()?)?);
        } else {
            return Err(
                meta.error("expected `request_volume_threshold`, `failure_ratio`, or `delay_ms`")
            );
        }
        Ok(())
    })?;

    Ok(circuit_breaker)
}

fn parse_bulkhead_attr(attr: &Attribute) -> syn::Result<BulkheadAttr> {
    let mut max_concurrent = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("max_concurrent") {
            max_concurrent = Some(parse_u64(&meta.value()?.parse()?)?);
        } else {
            return Err(meta.error("expected `max_concurrent`"));
        }
        Ok(())
    })?;
    let max_concurrent = max_concurrent.ok_or_else(|| {
        syn::Error::new_spanned(attr, "`#[bulkhead]` requires `max_concurrent = ...`")
    })?;
    Ok(BulkheadAttr { max_concurrent })
}

fn reject_duplicate(seen: bool, attr: &Attribute, name: &str) -> syn::Result<()> {
    if seen {
        Err(syn::Error::new_spanned(
            attr,
            format!("duplicate `#[{name}]` attribute"),
        ))
    } else {
        Ok(())
    }
}

fn parse_u64(expr: &Expr) -> syn::Result<u64> {
    match expr {
        Expr::Lit(expr_lit) => match &expr_lit.lit {
            Lit::Int(value) => parse_lit_int(value),
            _ => Err(syn::Error::new_spanned(expr, "expected integer literal")),
        },
        _ => Err(syn::Error::new_spanned(expr, "expected integer literal")),
    }
}

fn parse_lit_int(value: &LitInt) -> syn::Result<u64> {
    value.base10_parse()
}

fn parse_f64(expr: &Expr) -> syn::Result<f64> {
    match expr {
        Expr::Lit(expr_lit) => match &expr_lit.lit {
            Lit::Float(value) => parse_lit_float(value),
            Lit::Int(value) => Ok(parse_lit_int(value)? as f64),
            _ => Err(syn::Error::new_spanned(expr, "expected float literal")),
        },
        _ => Err(syn::Error::new_spanned(expr, "expected float literal")),
    }
}

fn parse_lit_float(value: &LitFloat) -> syn::Result<f64> {
    value.base10_parse()
}
