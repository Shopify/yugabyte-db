# Online schema change diagrams

PlantUML source files are authoritative. Render SVGs from this directory with:

```bash
plantuml -tsvg -o rendered *.puml
```

Check syntax without rendering:

```bash
plantuml -checkonly *.puml
```

Keep diagram names numbered in reading order. Commit both source and rendered
SVG so architecture documents render on GitHub without a PlantUML extension.
