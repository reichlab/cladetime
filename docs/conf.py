import os
import sys
from datetime import date

# Configuration file for the Sphinx documentation builder.

# -- Project information

project = "Cladetime"
project_copyright = f"{date.today().year}, Reich Lab @ The University of Massachusetts Amherst"
author = "Reich Lab"

# Add cladetime location to the path, so we can use autodoc to
# generate API documentation from docstrings.
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, root_path)

release = "0.1"
# FIXME: get the version dynamically
version = "0.1.0"

# -- General configuration

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx_copybutton",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_github_style",
    "sphinxext.opengraph",
    "sphinx.ext.napoleon",
    "sphinx_toolbox.github",
    "sphinx_toolbox.sidebar_links",
]

github_username = "reichlab"
github_repository = "cladetime"

intersphinx_mapping = {
    "ncov": ("https://docs.nextstrain.org/projects/ncov/en/latest/", None),
    "nextstrain": ("https://docs.nextstrain.org/en/latest", None),
    "nextclade": ("https://docs.nextstrain.org/projects/nextclade/en/stable/", None),
    "polars": ("https://docs.pola.rs/api/python/stable", None),
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

# Copied these settings from the copybutton's config
# https://github.com/executablebooks/sphinx-copybutton/blob/master/docs/conf.py
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True
copybutton_line_continuation_character = "\\"
copybutton_here_doc_delimiter = "EOT"
copybutton_selector = "div:not(.no-copybutton) > div.highlight > pre"

templates_path = ["_templates"]

# The root toctree document.
root_doc = "index"

# Test code blocks only when explicitly specified
doctest_test_doctest_blocks = ""

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_static_path = ["_static"]
html_theme = "furo"
html_favicon = "_static/reichlab_favicon.png"
html_title = "Cladetime"
html_last_updated_fmt = "%Y-%m-%d"

# Settings for the GitHub link extension
linkcode_url = "https://github.com/reichlab/cladetime"

# These folders are copied to the documentation's HTML output
html_theme_options = {
    "announcement": """
        <a style=\"text-decoration: none; color: white;\"
           href=\"https://github.com/reichlab/cladetime/issues">
           Cladetime is a work in progress. Please feel free to file issues on GitHub.
        </a>
    """,
    "sidebar_hide_name": True,
    "light_logo": "cladetime_logo_light_mode.png",
    "dark_logo": "cladetime_logo_dark_mode.png",
    "navigation_with_keys": True,
    "source_repository": "https://github.com/reichlab/cladetime/",
    # source for GitHub footer icon:
    # https://pradyunsg.me/furo/customisation/footer/#using-embedded-svgs
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/reichlab/cladetime",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path>
                </svg>
            """,
            "class": "",
        },
    ],
}

# from https://myst-parser.readthedocs.io/en/latest/syntax/optional.html
myst_enable_extensions = [
    "amsmath",
    "deflist",
    "dollarmath",
    "fieldlist",
    "substitution",
    "tasklist",
    "colon_fence",
    "attrs_inline",
]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "friendly"

# Show typehints as content of the function or method
autodoc_typehints = "signature"
autodoc_member_order = "bysource"


# Open Graph metadata
ogp_site_url = "https://cladetime.readthedocs.io"
ogp_title = "cladetime documentation"
ogp_type = "website"
ogp_image = "https://cladetime.readthedocs.io/en/latest/_static/cladetime_logo_light_mode.png"
ogp_social_cards = {
    "image": "https://cladetime.readthedocs.io/en/latest/_static/cladetime_logo_light_mode.png",
    "line_color": "#5d9c9c",
}

# Warn about all references to unknown targets
nitpicky = True
nitpick_ignore = [
    ("py:class", "datetime"),
    ("py:class", "polars.LazyFrame"),
    ("py:class", "polars.dataframe.frame.DataFrame"),
    ("py:class", "polars.DataFrame"),
    ("py:class", "polars.lazyframe.frame.LazyFrame"),
    ("py:class", "cladetime._clade.Clade"),
    ("py:class", "Clade"),
]


# -- Options for EPUB output
epub_show_urls = "footnote"
