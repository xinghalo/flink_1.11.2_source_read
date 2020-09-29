/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} node
 * @return {?}
 */
export function scrollIntoView(node) {
    /** @type {?} */
    const nodeAsAny = (/** @type {?} */ (node));
    if (nodeAsAny.scrollIntoViewIfNeeded) {
        /* tslint:disable-next-line:no-string-literal */
        nodeAsAny.scrollIntoViewIfNeeded(false);
        return;
    }
    if (node.scrollIntoView) {
        node.scrollIntoView(false);
        return;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2Nyb2xsLWludG8tdmlldy1pZi1uZWVkZWQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS91dGlsL3Njcm9sbC1pbnRvLXZpZXctaWYtbmVlZGVkLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7O0FBQUEsTUFBTSxVQUFVLGNBQWMsQ0FBQyxJQUFpQjs7VUFDeEMsU0FBUyxHQUFHLG1CQUFBLElBQUksRUFBTztJQUM3QixJQUFJLFNBQVMsQ0FBQyxzQkFBc0IsRUFBRTtRQUNwQyxnREFBZ0Q7UUFDaEQsU0FBUyxDQUFDLHNCQUFzQixDQUFDLEtBQUssQ0FBQyxDQUFDO1FBQ3hDLE9BQU87S0FDUjtJQUNELElBQUksSUFBSSxDQUFDLGNBQWMsRUFBRTtRQUN2QixJQUFJLENBQUMsY0FBYyxDQUFDLEtBQUssQ0FBQyxDQUFDO1FBQzNCLE9BQU87S0FDUjtBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJleHBvcnQgZnVuY3Rpb24gc2Nyb2xsSW50b1ZpZXcobm9kZTogSFRNTEVsZW1lbnQpOiB2b2lkIHtcbiAgY29uc3Qgbm9kZUFzQW55ID0gbm9kZSBhcyBhbnk7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gIGlmIChub2RlQXNBbnkuc2Nyb2xsSW50b1ZpZXdJZk5lZWRlZCkge1xuICAgIC8qIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1zdHJpbmctbGl0ZXJhbCAqL1xuICAgIG5vZGVBc0FueS5zY3JvbGxJbnRvVmlld0lmTmVlZGVkKGZhbHNlKTtcbiAgICByZXR1cm47XG4gIH1cbiAgaWYgKG5vZGUuc2Nyb2xsSW50b1ZpZXcpIHtcbiAgICBub2RlLnNjcm9sbEludG9WaWV3KGZhbHNlKTtcbiAgICByZXR1cm47XG4gIH1cbn1cbiJdfQ==