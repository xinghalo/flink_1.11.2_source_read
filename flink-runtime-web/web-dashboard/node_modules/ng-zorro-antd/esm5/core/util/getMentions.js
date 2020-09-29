/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} prefix
 * @return {?}
 */
export function getRegExp(prefix) {
    /** @type {?} */
    var prefixArray = Array.isArray(prefix) ? prefix : [prefix];
    /** @type {?} */
    var prefixToken = prefixArray.join('').replace(/(\$|\^)/g, '\\$1');
    if (prefixArray.length > 1) {
        prefixToken = "[" + prefixToken + "]";
    }
    return new RegExp("(\\s|^)(" + prefixToken + ")[^\\s]*", 'g');
}
/**
 * @param {?} value
 * @param {?=} prefix
 * @return {?}
 */
export function getMentions(value, prefix) {
    if (prefix === void 0) { prefix = '@'; }
    if (typeof value !== 'string') {
        return [];
    }
    /** @type {?} */
    var regex = getRegExp(prefix);
    /** @type {?} */
    var mentions = value.match(regex);
    return mentions !== null ? mentions.map((/**
     * @param {?} e
     * @return {?}
     */
    function (e) { return e.trim(); })) : [];
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZ2V0TWVudGlvbnMuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS91dGlsL2dldE1lbnRpb25zLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7O0FBQUEsTUFBTSxVQUFVLFNBQVMsQ0FBQyxNQUF5Qjs7UUFDM0MsV0FBVyxHQUFHLEtBQUssQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUM7O1FBQ3pELFdBQVcsR0FBRyxXQUFXLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFVLEVBQUUsTUFBTSxDQUFDO0lBRWxFLElBQUksV0FBVyxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUU7UUFDMUIsV0FBVyxHQUFHLE1BQUksV0FBVyxNQUFHLENBQUM7S0FDbEM7SUFFRCxPQUFPLElBQUksTUFBTSxDQUFDLGFBQVcsV0FBVyxhQUFVLEVBQUUsR0FBRyxDQUFDLENBQUM7QUFDM0QsQ0FBQzs7Ozs7O0FBRUQsTUFBTSxVQUFVLFdBQVcsQ0FBQyxLQUFhLEVBQUUsTUFBK0I7SUFBL0IsdUJBQUEsRUFBQSxZQUErQjtJQUN4RSxJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsRUFBRTtRQUM3QixPQUFPLEVBQUUsQ0FBQztLQUNYOztRQUNLLEtBQUssR0FBRyxTQUFTLENBQUMsTUFBTSxDQUFDOztRQUN6QixRQUFRLEdBQUcsS0FBSyxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUM7SUFDbkMsT0FBTyxRQUFRLEtBQUssSUFBSSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsR0FBRzs7OztJQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLElBQUksRUFBRSxFQUFSLENBQVEsRUFBQyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUM7QUFDOUQsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImV4cG9ydCBmdW5jdGlvbiBnZXRSZWdFeHAocHJlZml4OiBzdHJpbmcgfCBzdHJpbmdbXSk6IFJlZ0V4cCB7XG4gIGNvbnN0IHByZWZpeEFycmF5ID0gQXJyYXkuaXNBcnJheShwcmVmaXgpID8gcHJlZml4IDogW3ByZWZpeF07XG4gIGxldCBwcmVmaXhUb2tlbiA9IHByZWZpeEFycmF5LmpvaW4oJycpLnJlcGxhY2UoLyhcXCR8XFxeKS9nLCAnXFxcXCQxJyk7XG5cbiAgaWYgKHByZWZpeEFycmF5Lmxlbmd0aCA+IDEpIHtcbiAgICBwcmVmaXhUb2tlbiA9IGBbJHtwcmVmaXhUb2tlbn1dYDtcbiAgfVxuXG4gIHJldHVybiBuZXcgUmVnRXhwKGAoXFxcXHN8XikoJHtwcmVmaXhUb2tlbn0pW15cXFxcc10qYCwgJ2cnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbnRpb25zKHZhbHVlOiBzdHJpbmcsIHByZWZpeDogc3RyaW5nIHwgc3RyaW5nW10gPSAnQCcpOiBzdHJpbmdbXSB7XG4gIGlmICh0eXBlb2YgdmFsdWUgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuIFtdO1xuICB9XG4gIGNvbnN0IHJlZ2V4ID0gZ2V0UmVnRXhwKHByZWZpeCk7XG4gIGNvbnN0IG1lbnRpb25zID0gdmFsdWUubWF0Y2gocmVnZXgpO1xuICByZXR1cm4gbWVudGlvbnMgIT09IG51bGwgPyBtZW50aW9ucy5tYXAoZSA9PiBlLnRyaW0oKSkgOiBbXTtcbn1cbiJdfQ==