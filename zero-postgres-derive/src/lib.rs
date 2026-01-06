use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Meta, parse_macro_input};

/// Derive macro for `FromRow` trait.
///
/// Generates an implementation that matches column names to struct fields.
///
/// # Example
///
/// ```ignore
/// #[derive(FromRow)]
/// struct User {
///     name: String,
///     age: i32,
/// }
/// ```
///
/// # Strict Mode
///
/// By default, unknown columns are silently skipped. Use `#[from_row(strict)]`
/// to error on unknown columns:
///
/// ```ignore
/// #[derive(FromRow)]
/// #[from_row(strict)]
/// struct User {
///     name: String,
///     age: i32,
/// }
/// ```
#[proc_macro_derive(FromRow, attributes(from_row))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Check for #[from_row(strict)]
    let strict = input.attrs.iter().any(|attr| {
        if !attr.path().is_ident("from_row") {
            return false;
        }
        match &attr.meta {
            Meta::List(list) => list.tokens.to_string().contains("strict"),
            _ => false,
        }
    });

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("FromRow only supports structs with named fields"),
        },
        _ => panic!("FromRow only supports structs"),
    };

    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let field_name_strs: Vec<_> = field_names.iter().map(|n| n.to_string()).collect();

    // Generate MaybeUninit declarations
    let uninit_decls = field_names
        .iter()
        .zip(field_types.iter())
        .map(|(name, ty)| {
            quote! {
                let mut #name: ::core::mem::MaybeUninit<#ty> = ::core::mem::MaybeUninit::uninit();
            }
        });

    // Generate set flags
    let set_flag_names: Vec<_> = field_names
        .iter()
        .map(|n| syn::Ident::new(&format!("{}_set", n), n.span()))
        .collect();

    let set_flag_decls = set_flag_names.iter().map(|flag| {
        quote! { let mut #flag = false; }
    });

    // Generate match arms for text decoding
    let match_arms_text = field_names.iter().zip(field_types.iter()).zip(set_flag_names.iter()).zip(field_name_strs.iter()).map(|(((name, ty), flag), name_str)| {
        quote! {
            #name_str => {
                let __val: #ty = match __value {
                    None => ::zero_postgres::conversion::FromWireValue::from_null()?,
                    Some(__bytes) => ::zero_postgres::conversion::FromWireValue::from_text(__field.type_oid(), __bytes)?,
                };
                #name.write(__val);
                #flag = true;
            }
        }
    });

    // Generate match arms for binary decoding
    let match_arms_binary = field_names.iter().zip(field_types.iter()).zip(set_flag_names.iter()).zip(field_name_strs.iter()).map(|(((name, ty), flag), name_str)| {
        quote! {
            #name_str => {
                let __val: #ty = match __value {
                    None => ::zero_postgres::conversion::FromWireValue::from_null()?,
                    Some(__bytes) => ::zero_postgres::conversion::FromWireValue::from_binary(__field.type_oid(), __bytes)?,
                };
                #name.write(__val);
                #flag = true;
            }
        }
    });

    // Generate fallback arm based on strict mode
    let fallback_arm = if strict {
        quote! {
            __unknown => {
                return Err(::zero_postgres::Error::Decode(format!("unknown column: {}", __unknown)));
            }
        }
    } else {
        quote! {
            _ => {
                // Skip unknown column
            }
        }
    };

    // Generate initialization checks
    let init_checks = field_names
        .iter()
        .zip(set_flag_names.iter())
        .zip(field_name_strs.iter())
        .map(|((_name, flag), name_str)| {
            quote! {
                if !#flag {
                    return Err(::zero_postgres::Error::Decode(format!("missing column: {}", #name_str)));
                }
            }
        });

    // Generate struct construction
    let field_inits = field_names.iter().map(|name| {
        quote! {
            #name: unsafe { #name.assume_init() }
        }
    });

    // Clone iterators for text implementation
    let uninit_decls_text = uninit_decls.clone();
    let set_flag_decls_text = set_flag_decls.clone();
    let init_checks_text = init_checks.clone();
    let field_inits_text = field_inits.clone();

    // Clone for binary implementation
    let uninit_decls_binary = field_names
        .iter()
        .zip(field_types.iter())
        .map(|(name, ty)| {
            quote! {
                let mut #name: ::core::mem::MaybeUninit<#ty> = ::core::mem::MaybeUninit::uninit();
            }
        });

    let set_flag_decls_binary = set_flag_names.iter().map(|flag| {
        quote! { let mut #flag = false; }
    });

    let init_checks_binary = field_names
        .iter()
        .zip(set_flag_names.iter())
        .zip(field_name_strs.iter())
        .map(|((_name, flag), name_str)| {
            quote! {
                if !#flag {
                    return Err(::zero_postgres::Error::Decode(format!("missing column: {}", #name_str)));
                }
            }
        });

    let field_inits_binary = field_names.iter().map(|name| {
        quote! {
            #name: unsafe { #name.assume_init() }
        }
    });

    let expanded = quote! {
        impl #impl_generics ::zero_postgres::conversion::FromRow<'_> for #name #ty_generics #where_clause {
            fn from_row_text(
                __cols: &[::zero_postgres::protocol::backend::query::FieldDescription],
                __row: ::zero_postgres::protocol::backend::query::DataRow<'_>,
            ) -> ::zero_postgres::Result<Self> {
                #(#uninit_decls_text)*
                #(#set_flag_decls_text)*

                let mut __values = __row.iter();

                for __field in __cols.iter() {
                    let __value = __values.next().flatten();
                    let __col_name = __field.name;
                    match __col_name {
                        #(#match_arms_text)*
                        #fallback_arm
                    }
                }

                #(#init_checks_text)*

                Ok(Self {
                    #(#field_inits_text),*
                })
            }

            fn from_row_binary(
                __cols: &[::zero_postgres::protocol::backend::query::FieldDescription],
                __row: ::zero_postgres::protocol::backend::query::DataRow<'_>,
            ) -> ::zero_postgres::Result<Self> {
                #(#uninit_decls_binary)*
                #(#set_flag_decls_binary)*

                let mut __values = __row.iter();

                for __field in __cols.iter() {
                    let __value = __values.next().flatten();
                    let __col_name = __field.name;
                    match __col_name {
                        #(#match_arms_binary)*
                        #fallback_arm
                    }
                }

                #(#init_checks_binary)*

                Ok(Self {
                    #(#field_inits_binary),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
