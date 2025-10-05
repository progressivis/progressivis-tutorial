# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import anywidget
import traitlets


class ImageWidget(anywidget.AnyWidget):
    _esm = """
    function render({ model, el }) {
      let img = document.createElement("img");
      img.src = model.get("url");
      img.addEventListener("wheel", function (event) {
        event.preventDefault();
        model.set("scale", model.get("scale") + event.deltaY * -0.01);
        model.save_changes();
      });
      model.on("change:url", () => {
        img.src = model.get("url");
      });
      el.classList.add("image-widget");
      el.appendChild(img);
    }
    export default { render };
    """
    _css = """
    """
    url = traitlets.Unicode("").tag(sync=True)
    scale = traitlets.Float(1.0).tag(sync=True)



# %%

img = ImageWidget(url="https://www.aviz.fr/wiki/uploads/Progressive/construction_tour_eiffel.jpg")
img

# %%
img.url = "https://www.aviz.fr/wiki/uploads/Progressive/PDAV-first-page.png"

# %%
img.scale
