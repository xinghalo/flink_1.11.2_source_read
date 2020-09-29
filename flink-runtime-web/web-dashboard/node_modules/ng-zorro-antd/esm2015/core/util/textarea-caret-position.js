/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
// from https://github.com/component/textarea-caret-position
// We'll copy the properties below into the mirror div.
// Note that some browsers, such as Firefox, do not concatenate properties
// into their shorthand (e.g. padding-top, padding-bottom etc. -> padding),
// so we have to list every single property explicitly.
/** @type {?} */
export const properties = [
    'direction',
    'boxSizing',
    'width',
    'height',
    'overflowX',
    'overflowY',
    'borderTopWidth',
    'borderRightWidth',
    'borderBottomWidth',
    'borderLeftWidth',
    'borderStyle',
    'paddingTop',
    'paddingRight',
    'paddingBottom',
    'paddingLeft',
    // https://developer.mozilla.org/en-US/docs/Web/CSS/font
    'fontStyle',
    'fontVariant',
    'fontWeight',
    'fontStretch',
    'fontSize',
    'fontSizeAdjust',
    'lineHeight',
    'fontFamily',
    'textAlign',
    'textTransform',
    'textIndent',
    'textDecoration',
    'letterSpacing',
    'wordSpacing',
    'tabSize',
    'MozTabSize'
];
/** @type {?} */
const isBrowser = (typeof window !== 'undefined');
// tslint:disable-next-line:no-any
/** @type {?} */
const isFirefox = (isBrowser && ((/** @type {?} */ (window))).mozInnerScreenX != null);
/** @type {?} */
const _parseInt = (/**
 * @param {?} str
 * @return {?}
 */
(str) => parseInt(str, 10));
const ɵ0 = _parseInt;
/**
 * @record
 */
export function Coordinates() { }
if (false) {
    /** @type {?} */
    Coordinates.prototype.top;
    /** @type {?} */
    Coordinates.prototype.left;
    /** @type {?} */
    Coordinates.prototype.height;
}
/**
 * @param {?} element
 * @param {?} position
 * @param {?=} options
 * @return {?}
 */
export function getCaretCoordinates(element, position, options) {
    if (!isBrowser) {
        throw new Error('textarea-caret-position#getCaretCoordinates should only be called in a browser');
    }
    /** @type {?} */
    const debug = options && options.debug || false;
    if (debug) {
        /** @type {?} */
        const el = document.querySelector('#input-textarea-caret-position-mirror-div');
        if (el) {
            (/** @type {?} */ (el.parentNode)).removeChild(el);
        }
    }
    // The mirror div will replicate the textarea's style
    /** @type {?} */
    const div = document.createElement('div');
    div.id = 'input-textarea-caret-position-mirror-div';
    document.body.appendChild(div);
    /** @type {?} */
    const style = div.style;
    // tslint:disable-next-line:no-any
    /** @type {?} */
    const computed = window.getComputedStyle ? window.getComputedStyle(element) : ((/** @type {?} */ (element))).currentStyle;
    // currentStyle for IE < 9
    /** @type {?} */
    const isInput = element.nodeName === 'INPUT';
    // Default textarea styles
    style.whiteSpace = 'pre-wrap';
    if (!isInput) {
        style.wordWrap = 'break-word'; // only for textarea-s
    }
    // Position off-screen
    style.position = 'absolute'; // required to return coordinates properly
    if (!debug) {
        style.visibility = 'hidden';
    } // not 'display: none' because we want rendering
    // Transfer the element's properties to the div
    properties.forEach((/**
     * @param {?} prop
     * @return {?}
     */
    (prop) => {
        if (isInput && prop === 'lineHeight') {
            // Special case for <input>s because text is rendered centered and line height may be != height
            style.lineHeight = computed.height;
        }
        else {
            // @ts-ignore
            style[prop] = computed[prop];
        }
    }));
    if (isFirefox) {
        // Firefox lies about the overflow property for textareas: https://bugzilla.mozilla.org/show_bug.cgi?id=984275
        if (element.scrollHeight > _parseInt(computed.height)) {
            style.overflowY = 'scroll';
        }
    }
    else {
        style.overflow = 'hidden'; // for Chrome to not render a scrollbar; IE keeps overflowY = 'scroll'
    }
    div.textContent = element.value.substring(0, position);
    // The second special handling for input type="text" vs textarea:
    // spaces need to be replaced with non-breaking spaces - http://stackoverflow.com/a/13402035/1269037
    if (isInput) {
        div.textContent = div.textContent.replace(/\s/g, '\u00a0');
    }
    /** @type {?} */
    const span = document.createElement('span');
    // Wrapping must be replicated *exactly*, including when a long word gets
    // onto the next line, with whitespace at the end of the line before (#7).
    // The  *only* reliable way to do that is to copy the *entire* rest of the
    // textarea's content into the <span> created at the caret position.
    // For inputs, just '.' would be enough, but no need to bother.
    span.textContent = element.value.substring(position) || '.'; // || because a completely empty faux span doesn't render at all
    div.appendChild(span);
    /** @type {?} */
    const coordinates = {
        top: span.offsetTop + _parseInt(computed.borderTopWidth),
        left: span.offsetLeft + _parseInt(computed.borderLeftWidth),
        height: _parseInt(computed.lineHeight)
    };
    if (debug) {
        span.style.backgroundColor = '#eee';
        createDebugEle(element, coordinates);
    }
    else {
        document.body.removeChild(div);
    }
    return coordinates;
}
/**
 * @param {?} element
 * @param {?} coordinates
 * @return {?}
 */
export function createDebugEle(element, coordinates) {
    /** @type {?} */
    const fontSize = getComputedStyle(element).getPropertyValue('font-size');
    /** @type {?} */
    const rect = ((/** @type {?} */ (document.querySelector('#DEBUG'))))
        || document.createElement('div');
    document.body.appendChild(rect);
    rect.id = 'DEBUG';
    rect.style.position = 'absolute';
    rect.style.backgroundColor = 'red';
    rect.style.height = fontSize;
    rect.style.width = '1px';
    rect.style.top = `${element.getBoundingClientRect().top - element.scrollTop + window.pageYOffset + coordinates.top}px`;
    rect.style.left = `${element.getBoundingClientRect().left - element.scrollLeft + window.pageXOffset + coordinates.left}px`;
    console.log(rect.style.top);
    console.log(rect.style.left);
}
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGV4dGFyZWEtY2FyZXQtcG9zaXRpb24uanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS91dGlsL3RleHRhcmVhLWNhcmV0LXBvc2l0aW9uLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7QUFNQSxNQUFNLE9BQU8sVUFBVSxHQUFHO0lBQ3hCLFdBQVc7SUFDWCxXQUFXO0lBQ1gsT0FBTztJQUNQLFFBQVE7SUFDUixXQUFXO0lBQ1gsV0FBVztJQUVYLGdCQUFnQjtJQUNoQixrQkFBa0I7SUFDbEIsbUJBQW1CO0lBQ25CLGlCQUFpQjtJQUNqQixhQUFhO0lBRWIsWUFBWTtJQUNaLGNBQWM7SUFDZCxlQUFlO0lBQ2YsYUFBYTtJQUViLHdEQUF3RDtJQUN4RCxXQUFXO0lBQ1gsYUFBYTtJQUNiLFlBQVk7SUFDWixhQUFhO0lBQ2IsVUFBVTtJQUNWLGdCQUFnQjtJQUNoQixZQUFZO0lBQ1osWUFBWTtJQUVaLFdBQVc7SUFDWCxlQUFlO0lBQ2YsWUFBWTtJQUNaLGdCQUFnQjtJQUVoQixlQUFlO0lBQ2YsYUFBYTtJQUViLFNBQVM7SUFDVCxZQUFZO0NBRWI7O01BRUssU0FBUyxHQUFHLENBQUMsT0FBTyxNQUFNLEtBQUssV0FBVyxDQUFDOzs7TUFHM0MsU0FBUyxHQUFHLENBQUMsU0FBUyxJQUFJLENBQUMsbUJBQUEsTUFBTSxFQUFPLENBQUMsQ0FBQyxlQUFlLElBQUksSUFBSSxDQUFDOztNQUVsRSxTQUFTOzs7O0FBQUcsQ0FBQyxHQUFXLEVBQUUsRUFBRSxDQUFDLFFBQVEsQ0FBQyxHQUFHLEVBQUUsRUFBRSxDQUFDLENBQUE7Ozs7O0FBRXBELGlDQUlDOzs7SUFIQywwQkFBWTs7SUFDWiwyQkFBYTs7SUFDYiw2QkFBZTs7Ozs7Ozs7QUFHakIsTUFBTSxVQUFVLG1CQUFtQixDQUFDLE9BQStDLEVBQUUsUUFBZ0IsRUFBRSxPQUE2QjtJQUNsSSxJQUFJLENBQUMsU0FBUyxFQUFFO1FBQ2QsTUFBTSxJQUFJLEtBQUssQ0FBQyxnRkFBZ0YsQ0FBQyxDQUFDO0tBQ25HOztVQUVLLEtBQUssR0FBRyxPQUFPLElBQUksT0FBTyxDQUFDLEtBQUssSUFBSSxLQUFLO0lBQy9DLElBQUksS0FBSyxFQUFFOztjQUNILEVBQUUsR0FBRyxRQUFRLENBQUMsYUFBYSxDQUFDLDJDQUEyQyxDQUFDO1FBQzlFLElBQUksRUFBRSxFQUFFO1lBQUUsbUJBQUEsRUFBRSxDQUFDLFVBQVUsRUFBQyxDQUFDLFdBQVcsQ0FBQyxFQUFFLENBQUMsQ0FBQztTQUFFO0tBQzVDOzs7VUFHSyxHQUFHLEdBQUcsUUFBUSxDQUFDLGFBQWEsQ0FBQyxLQUFLLENBQUM7SUFDekMsR0FBRyxDQUFDLEVBQUUsR0FBRywwQ0FBMEMsQ0FBQztJQUNwRCxRQUFRLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxHQUFHLENBQUMsQ0FBQzs7VUFFekIsS0FBSyxHQUFHLEdBQUcsQ0FBQyxLQUFLOzs7VUFHakIsUUFBUSxHQUFHLE1BQU0sQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLGdCQUFnQixDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLG1CQUFBLE9BQU8sRUFBTyxDQUFDLENBQUMsWUFBWTs7O1VBQ3JHLE9BQU8sR0FBRyxPQUFPLENBQUMsUUFBUSxLQUFLLE9BQU87SUFFNUMsMEJBQTBCO0lBQzFCLEtBQUssQ0FBQyxVQUFVLEdBQUcsVUFBVSxDQUFDO0lBQzlCLElBQUksQ0FBQyxPQUFPLEVBQUU7UUFDWixLQUFLLENBQUMsUUFBUSxHQUFHLFlBQVksQ0FBQyxDQUFDLHNCQUFzQjtLQUN0RDtJQUVELHNCQUFzQjtJQUN0QixLQUFLLENBQUMsUUFBUSxHQUFHLFVBQVUsQ0FBQyxDQUFFLDBDQUEwQztJQUN4RSxJQUFJLENBQUMsS0FBSyxFQUFFO1FBQ1YsS0FBSyxDQUFDLFVBQVUsR0FBRyxRQUFRLENBQUM7S0FDN0IsQ0FBRSxnREFBZ0Q7SUFFbkQsK0NBQStDO0lBQy9DLFVBQVUsQ0FBQyxPQUFPOzs7O0lBQUMsQ0FBQyxJQUFZLEVBQUUsRUFBRTtRQUNsQyxJQUFJLE9BQU8sSUFBSSxJQUFJLEtBQUssWUFBWSxFQUFFO1lBQ3BDLCtGQUErRjtZQUMvRixLQUFLLENBQUMsVUFBVSxHQUFHLFFBQVEsQ0FBQyxNQUFNLENBQUM7U0FDcEM7YUFBTTtZQUNMLGFBQWE7WUFDYixLQUFLLENBQUMsSUFBSSxDQUFDLEdBQUcsUUFBUSxDQUFDLElBQUksQ0FBQyxDQUFDO1NBQzlCO0lBQ0gsQ0FBQyxFQUFDLENBQUM7SUFFSCxJQUFJLFNBQVMsRUFBRTtRQUNiLDhHQUE4RztRQUM5RyxJQUFJLE9BQU8sQ0FBQyxZQUFZLEdBQUcsU0FBUyxDQUFDLFFBQVEsQ0FBQyxNQUFNLENBQUMsRUFBRTtZQUNyRCxLQUFLLENBQUMsU0FBUyxHQUFHLFFBQVEsQ0FBQztTQUM1QjtLQUNGO1NBQU07UUFDTCxLQUFLLENBQUMsUUFBUSxHQUFHLFFBQVEsQ0FBQyxDQUFFLHNFQUFzRTtLQUNuRztJQUVELEdBQUcsQ0FBQyxXQUFXLEdBQUcsT0FBTyxDQUFDLEtBQUssQ0FBQyxTQUFTLENBQUMsQ0FBQyxFQUFFLFFBQVEsQ0FBQyxDQUFDO0lBQ3ZELGlFQUFpRTtJQUNqRSxvR0FBb0c7SUFDcEcsSUFBSSxPQUFPLEVBQUU7UUFDWCxHQUFHLENBQUMsV0FBVyxHQUFHLEdBQUcsQ0FBQyxXQUFXLENBQUMsT0FBTyxDQUFDLEtBQUssRUFBRSxRQUFRLENBQUMsQ0FBQztLQUM1RDs7VUFFSyxJQUFJLEdBQUcsUUFBUSxDQUFDLGFBQWEsQ0FBQyxNQUFNLENBQUM7SUFDM0MseUVBQXlFO0lBQ3pFLDBFQUEwRTtJQUMxRSwwRUFBMEU7SUFDMUUsb0VBQW9FO0lBQ3BFLCtEQUErRDtJQUMvRCxJQUFJLENBQUMsV0FBVyxHQUFHLE9BQU8sQ0FBQyxLQUFLLENBQUMsU0FBUyxDQUFDLFFBQVEsQ0FBQyxJQUFJLEdBQUcsQ0FBQyxDQUFFLGdFQUFnRTtJQUM5SCxHQUFHLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxDQUFDOztVQUVoQixXQUFXLEdBQUc7UUFDbEIsR0FBRyxFQUFFLElBQUksQ0FBQyxTQUFTLEdBQUcsU0FBUyxDQUFDLFFBQVEsQ0FBQyxjQUFjLENBQUM7UUFDeEQsSUFBSSxFQUFFLElBQUksQ0FBQyxVQUFVLEdBQUcsU0FBUyxDQUFDLFFBQVEsQ0FBQyxlQUFlLENBQUM7UUFDM0QsTUFBTSxFQUFFLFNBQVMsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDO0tBQ3ZDO0lBRUQsSUFBSSxLQUFLLEVBQUU7UUFDVCxJQUFJLENBQUMsS0FBSyxDQUFDLGVBQWUsR0FBRyxNQUFNLENBQUM7UUFDcEMsY0FBYyxDQUFDLE9BQU8sRUFBRSxXQUFXLENBQUMsQ0FBQztLQUN0QztTQUFNO1FBQ0wsUUFBUSxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsR0FBRyxDQUFDLENBQUM7S0FDaEM7SUFFRCxPQUFPLFdBQVcsQ0FBQztBQUNyQixDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsY0FBYyxDQUFDLE9BQStDLEVBQUUsV0FBd0I7O1VBQ2hHLFFBQVEsR0FBRyxnQkFBZ0IsQ0FBQyxPQUFPLENBQUMsQ0FBQyxnQkFBZ0IsQ0FBQyxXQUFXLENBQUM7O1VBQ2xFLElBQUksR0FBb0IsQ0FBQyxtQkFBQSxRQUFRLENBQUMsYUFBYSxDQUFDLFFBQVEsQ0FBQyxFQUFtQixDQUFDO1dBQzlFLFFBQVEsQ0FBQyxhQUFhLENBQUMsS0FBSyxDQUFDO0lBQ2xDLFFBQVEsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxDQUFDO0lBQ2hDLElBQUksQ0FBQyxFQUFFLEdBQUcsT0FBTyxDQUFDO0lBQ2xCLElBQUksQ0FBQyxLQUFLLENBQUMsUUFBUSxHQUFHLFVBQVUsQ0FBQztJQUNqQyxJQUFJLENBQUMsS0FBSyxDQUFDLGVBQWUsR0FBRyxLQUFLLENBQUM7SUFDbkMsSUFBSSxDQUFDLEtBQUssQ0FBQyxNQUFNLEdBQUcsUUFBUSxDQUFDO0lBQzdCLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztJQUN6QixJQUFJLENBQUMsS0FBSyxDQUFDLEdBQUcsR0FBRyxHQUFHLE9BQU8sQ0FBQyxxQkFBcUIsRUFBRSxDQUFDLEdBQUcsR0FBRyxPQUFPLENBQUMsU0FBUyxHQUFHLE1BQU0sQ0FBQyxXQUFXLEdBQUcsV0FBVyxDQUFDLEdBQUcsSUFBSSxDQUFDO0lBQ3ZILElBQUksQ0FBQyxLQUFLLENBQUMsSUFBSSxHQUFHLEdBQUcsT0FBTyxDQUFDLHFCQUFxQixFQUFFLENBQUMsSUFBSSxHQUFHLE9BQU8sQ0FBQyxVQUFVLEdBQUcsTUFBTSxDQUFDLFdBQVcsR0FBRyxXQUFXLENBQUMsSUFBSSxJQUFJLENBQUM7SUFDM0gsT0FBTyxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDO0lBQzVCLE9BQU8sQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQztBQUMvQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLy8gZnJvbSBodHRwczovL2dpdGh1Yi5jb20vY29tcG9uZW50L3RleHRhcmVhLWNhcmV0LXBvc2l0aW9uXG5cbi8vIFdlJ2xsIGNvcHkgdGhlIHByb3BlcnRpZXMgYmVsb3cgaW50byB0aGUgbWlycm9yIGRpdi5cbi8vIE5vdGUgdGhhdCBzb21lIGJyb3dzZXJzLCBzdWNoIGFzIEZpcmVmb3gsIGRvIG5vdCBjb25jYXRlbmF0ZSBwcm9wZXJ0aWVzXG4vLyBpbnRvIHRoZWlyIHNob3J0aGFuZCAoZS5nLiBwYWRkaW5nLXRvcCwgcGFkZGluZy1ib3R0b20gZXRjLiAtPiBwYWRkaW5nKSxcbi8vIHNvIHdlIGhhdmUgdG8gbGlzdCBldmVyeSBzaW5nbGUgcHJvcGVydHkgZXhwbGljaXRseS5cbmV4cG9ydCBjb25zdCBwcm9wZXJ0aWVzID0gW1xuICAnZGlyZWN0aW9uJywgIC8vIFJUTCBzdXBwb3J0XG4gICdib3hTaXppbmcnLFxuICAnd2lkdGgnLCAgLy8gb24gQ2hyb21lIGFuZCBJRSwgZXhjbHVkZSB0aGUgc2Nyb2xsYmFyLCBzbyB0aGUgbWlycm9yIGRpdiB3cmFwcyBleGFjdGx5IGFzIHRoZSB0ZXh0YXJlYSBkb2VzXG4gICdoZWlnaHQnLFxuICAnb3ZlcmZsb3dYJyxcbiAgJ292ZXJmbG93WScsICAvLyBjb3B5IHRoZSBzY3JvbGxiYXIgZm9yIElFXG5cbiAgJ2JvcmRlclRvcFdpZHRoJyxcbiAgJ2JvcmRlclJpZ2h0V2lkdGgnLFxuICAnYm9yZGVyQm90dG9tV2lkdGgnLFxuICAnYm9yZGVyTGVmdFdpZHRoJyxcbiAgJ2JvcmRlclN0eWxlJyxcblxuICAncGFkZGluZ1RvcCcsXG4gICdwYWRkaW5nUmlnaHQnLFxuICAncGFkZGluZ0JvdHRvbScsXG4gICdwYWRkaW5nTGVmdCcsXG5cbiAgLy8gaHR0cHM6Ly9kZXZlbG9wZXIubW96aWxsYS5vcmcvZW4tVVMvZG9jcy9XZWIvQ1NTL2ZvbnRcbiAgJ2ZvbnRTdHlsZScsXG4gICdmb250VmFyaWFudCcsXG4gICdmb250V2VpZ2h0JyxcbiAgJ2ZvbnRTdHJldGNoJyxcbiAgJ2ZvbnRTaXplJyxcbiAgJ2ZvbnRTaXplQWRqdXN0JyxcbiAgJ2xpbmVIZWlnaHQnLFxuICAnZm9udEZhbWlseScsXG5cbiAgJ3RleHRBbGlnbicsXG4gICd0ZXh0VHJhbnNmb3JtJyxcbiAgJ3RleHRJbmRlbnQnLFxuICAndGV4dERlY29yYXRpb24nLCAgLy8gbWlnaHQgbm90IG1ha2UgYSBkaWZmZXJlbmNlLCBidXQgYmV0dGVyIGJlIHNhZmVcblxuICAnbGV0dGVyU3BhY2luZycsXG4gICd3b3JkU3BhY2luZycsXG5cbiAgJ3RhYlNpemUnLFxuICAnTW96VGFiU2l6ZSdcblxuXTtcblxuY29uc3QgaXNCcm93c2VyID0gKHR5cGVvZiB3aW5kb3cgIT09ICd1bmRlZmluZWQnKTtcblxuLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuY29uc3QgaXNGaXJlZm94ID0gKGlzQnJvd3NlciAmJiAod2luZG93IGFzIGFueSkubW96SW5uZXJTY3JlZW5YICE9IG51bGwpO1xuXG5jb25zdCBfcGFyc2VJbnQgPSAoc3RyOiBzdHJpbmcpID0+IHBhcnNlSW50KHN0ciwgMTApO1xuXG5leHBvcnQgaW50ZXJmYWNlIENvb3JkaW5hdGVzIHtcbiAgdG9wOiBudW1iZXI7XG4gIGxlZnQ6IG51bWJlcjtcbiAgaGVpZ2h0OiBudW1iZXI7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRDYXJldENvb3JkaW5hdGVzKGVsZW1lbnQ6IEhUTUxJbnB1dEVsZW1lbnQgfCBIVE1MVGV4dEFyZWFFbGVtZW50LCBwb3NpdGlvbjogbnVtYmVyLCBvcHRpb25zPzogeyBkZWJ1Zz86IGJvb2xlYW4gfSk6IENvb3JkaW5hdGVzIHtcbiAgaWYgKCFpc0Jyb3dzZXIpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ3RleHRhcmVhLWNhcmV0LXBvc2l0aW9uI2dldENhcmV0Q29vcmRpbmF0ZXMgc2hvdWxkIG9ubHkgYmUgY2FsbGVkIGluIGEgYnJvd3NlcicpO1xuICB9XG5cbiAgY29uc3QgZGVidWcgPSBvcHRpb25zICYmIG9wdGlvbnMuZGVidWcgfHwgZmFsc2U7XG4gIGlmIChkZWJ1Zykge1xuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQucXVlcnlTZWxlY3RvcignI2lucHV0LXRleHRhcmVhLWNhcmV0LXBvc2l0aW9uLW1pcnJvci1kaXYnKTtcbiAgICBpZiAoZWwpIHsgZWwucGFyZW50Tm9kZSEucmVtb3ZlQ2hpbGQoZWwpOyB9XG4gIH1cblxuICAvLyBUaGUgbWlycm9yIGRpdiB3aWxsIHJlcGxpY2F0ZSB0aGUgdGV4dGFyZWEncyBzdHlsZVxuICBjb25zdCBkaXYgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KCdkaXYnKTtcbiAgZGl2LmlkID0gJ2lucHV0LXRleHRhcmVhLWNhcmV0LXBvc2l0aW9uLW1pcnJvci1kaXYnO1xuICBkb2N1bWVudC5ib2R5LmFwcGVuZENoaWxkKGRpdik7XG5cbiAgY29uc3Qgc3R5bGUgPSBkaXYuc3R5bGU7XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBjb25zdCBjb21wdXRlZCA9IHdpbmRvdy5nZXRDb21wdXRlZFN0eWxlID8gd2luZG93LmdldENvbXB1dGVkU3R5bGUoZWxlbWVudCkgOiAoZWxlbWVudCBhcyBhbnkpLmN1cnJlbnRTdHlsZTsgIC8vIGN1cnJlbnRTdHlsZSBmb3IgSUUgPCA5XG4gIGNvbnN0IGlzSW5wdXQgPSBlbGVtZW50Lm5vZGVOYW1lID09PSAnSU5QVVQnO1xuXG4gIC8vIERlZmF1bHQgdGV4dGFyZWEgc3R5bGVzXG4gIHN0eWxlLndoaXRlU3BhY2UgPSAncHJlLXdyYXAnO1xuICBpZiAoIWlzSW5wdXQpIHtcbiAgICBzdHlsZS53b3JkV3JhcCA9ICdicmVhay13b3JkJzsgLy8gb25seSBmb3IgdGV4dGFyZWEtc1xuICB9XG5cbiAgLy8gUG9zaXRpb24gb2ZmLXNjcmVlblxuICBzdHlsZS5wb3NpdGlvbiA9ICdhYnNvbHV0ZSc7ICAvLyByZXF1aXJlZCB0byByZXR1cm4gY29vcmRpbmF0ZXMgcHJvcGVybHlcbiAgaWYgKCFkZWJ1Zykge1xuICAgIHN0eWxlLnZpc2liaWxpdHkgPSAnaGlkZGVuJztcbiAgfSAgLy8gbm90ICdkaXNwbGF5OiBub25lJyBiZWNhdXNlIHdlIHdhbnQgcmVuZGVyaW5nXG5cbiAgLy8gVHJhbnNmZXIgdGhlIGVsZW1lbnQncyBwcm9wZXJ0aWVzIHRvIHRoZSBkaXZcbiAgcHJvcGVydGllcy5mb3JFYWNoKChwcm9wOiBzdHJpbmcpID0+IHtcbiAgICBpZiAoaXNJbnB1dCAmJiBwcm9wID09PSAnbGluZUhlaWdodCcpIHtcbiAgICAgIC8vIFNwZWNpYWwgY2FzZSBmb3IgPGlucHV0PnMgYmVjYXVzZSB0ZXh0IGlzIHJlbmRlcmVkIGNlbnRlcmVkIGFuZCBsaW5lIGhlaWdodCBtYXkgYmUgIT0gaGVpZ2h0XG4gICAgICBzdHlsZS5saW5lSGVpZ2h0ID0gY29tcHV0ZWQuaGVpZ2h0O1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgICBzdHlsZVtwcm9wXSA9IGNvbXB1dGVkW3Byb3BdO1xuICAgIH1cbiAgfSk7XG5cbiAgaWYgKGlzRmlyZWZveCkge1xuICAgIC8vIEZpcmVmb3ggbGllcyBhYm91dCB0aGUgb3ZlcmZsb3cgcHJvcGVydHkgZm9yIHRleHRhcmVhczogaHR0cHM6Ly9idWd6aWxsYS5tb3ppbGxhLm9yZy9zaG93X2J1Zy5jZ2k/aWQ9OTg0Mjc1XG4gICAgaWYgKGVsZW1lbnQuc2Nyb2xsSGVpZ2h0ID4gX3BhcnNlSW50KGNvbXB1dGVkLmhlaWdodCkpIHtcbiAgICAgIHN0eWxlLm92ZXJmbG93WSA9ICdzY3JvbGwnO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBzdHlsZS5vdmVyZmxvdyA9ICdoaWRkZW4nOyAgLy8gZm9yIENocm9tZSB0byBub3QgcmVuZGVyIGEgc2Nyb2xsYmFyOyBJRSBrZWVwcyBvdmVyZmxvd1kgPSAnc2Nyb2xsJ1xuICB9XG5cbiAgZGl2LnRleHRDb250ZW50ID0gZWxlbWVudC52YWx1ZS5zdWJzdHJpbmcoMCwgcG9zaXRpb24pO1xuICAvLyBUaGUgc2Vjb25kIHNwZWNpYWwgaGFuZGxpbmcgZm9yIGlucHV0IHR5cGU9XCJ0ZXh0XCIgdnMgdGV4dGFyZWE6XG4gIC8vIHNwYWNlcyBuZWVkIHRvIGJlIHJlcGxhY2VkIHdpdGggbm9uLWJyZWFraW5nIHNwYWNlcyAtIGh0dHA6Ly9zdGFja292ZXJmbG93LmNvbS9hLzEzNDAyMDM1LzEyNjkwMzdcbiAgaWYgKGlzSW5wdXQpIHtcbiAgICBkaXYudGV4dENvbnRlbnQgPSBkaXYudGV4dENvbnRlbnQucmVwbGFjZSgvXFxzL2csICdcXHUwMGEwJyk7XG4gIH1cblxuICBjb25zdCBzcGFuID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnc3BhbicpO1xuICAvLyBXcmFwcGluZyBtdXN0IGJlIHJlcGxpY2F0ZWQgKmV4YWN0bHkqLCBpbmNsdWRpbmcgd2hlbiBhIGxvbmcgd29yZCBnZXRzXG4gIC8vIG9udG8gdGhlIG5leHQgbGluZSwgd2l0aCB3aGl0ZXNwYWNlIGF0IHRoZSBlbmQgb2YgdGhlIGxpbmUgYmVmb3JlICgjNykuXG4gIC8vIFRoZSAgKm9ubHkqIHJlbGlhYmxlIHdheSB0byBkbyB0aGF0IGlzIHRvIGNvcHkgdGhlICplbnRpcmUqIHJlc3Qgb2YgdGhlXG4gIC8vIHRleHRhcmVhJ3MgY29udGVudCBpbnRvIHRoZSA8c3Bhbj4gY3JlYXRlZCBhdCB0aGUgY2FyZXQgcG9zaXRpb24uXG4gIC8vIEZvciBpbnB1dHMsIGp1c3QgJy4nIHdvdWxkIGJlIGVub3VnaCwgYnV0IG5vIG5lZWQgdG8gYm90aGVyLlxuICBzcGFuLnRleHRDb250ZW50ID0gZWxlbWVudC52YWx1ZS5zdWJzdHJpbmcocG9zaXRpb24pIHx8ICcuJzsgIC8vIHx8IGJlY2F1c2UgYSBjb21wbGV0ZWx5IGVtcHR5IGZhdXggc3BhbiBkb2Vzbid0IHJlbmRlciBhdCBhbGxcbiAgZGl2LmFwcGVuZENoaWxkKHNwYW4pO1xuXG4gIGNvbnN0IGNvb3JkaW5hdGVzID0ge1xuICAgIHRvcDogc3Bhbi5vZmZzZXRUb3AgKyBfcGFyc2VJbnQoY29tcHV0ZWQuYm9yZGVyVG9wV2lkdGgpLFxuICAgIGxlZnQ6IHNwYW4ub2Zmc2V0TGVmdCArIF9wYXJzZUludChjb21wdXRlZC5ib3JkZXJMZWZ0V2lkdGgpLFxuICAgIGhlaWdodDogX3BhcnNlSW50KGNvbXB1dGVkLmxpbmVIZWlnaHQpXG4gIH07XG5cbiAgaWYgKGRlYnVnKSB7XG4gICAgc3Bhbi5zdHlsZS5iYWNrZ3JvdW5kQ29sb3IgPSAnI2VlZSc7XG4gICAgY3JlYXRlRGVidWdFbGUoZWxlbWVudCwgY29vcmRpbmF0ZXMpO1xuICB9IGVsc2Uge1xuICAgIGRvY3VtZW50LmJvZHkucmVtb3ZlQ2hpbGQoZGl2KTtcbiAgfVxuXG4gIHJldHVybiBjb29yZGluYXRlcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZURlYnVnRWxlKGVsZW1lbnQ6IEhUTUxJbnB1dEVsZW1lbnQgfCBIVE1MVGV4dEFyZWFFbGVtZW50LCBjb29yZGluYXRlczogQ29vcmRpbmF0ZXMpOiB2b2lkIHtcbiAgY29uc3QgZm9udFNpemUgPSBnZXRDb21wdXRlZFN0eWxlKGVsZW1lbnQpLmdldFByb3BlcnR5VmFsdWUoJ2ZvbnQtc2l6ZScpO1xuICBjb25zdCByZWN0OiBIVE1MU3BhbkVsZW1lbnQgPSAoZG9jdW1lbnQucXVlcnlTZWxlY3RvcignI0RFQlVHJykgYXMgSFRNTFNwYW5FbGVtZW50KVxuICAgIHx8IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2RpdicpO1xuICBkb2N1bWVudC5ib2R5LmFwcGVuZENoaWxkKHJlY3QpO1xuICByZWN0LmlkID0gJ0RFQlVHJztcbiAgcmVjdC5zdHlsZS5wb3NpdGlvbiA9ICdhYnNvbHV0ZSc7XG4gIHJlY3Quc3R5bGUuYmFja2dyb3VuZENvbG9yID0gJ3JlZCc7XG4gIHJlY3Quc3R5bGUuaGVpZ2h0ID0gZm9udFNpemU7XG4gIHJlY3Quc3R5bGUud2lkdGggPSAnMXB4JztcbiAgcmVjdC5zdHlsZS50b3AgPSBgJHtlbGVtZW50LmdldEJvdW5kaW5nQ2xpZW50UmVjdCgpLnRvcCAtIGVsZW1lbnQuc2Nyb2xsVG9wICsgd2luZG93LnBhZ2VZT2Zmc2V0ICsgY29vcmRpbmF0ZXMudG9wfXB4YDtcbiAgcmVjdC5zdHlsZS5sZWZ0ID0gYCR7ZWxlbWVudC5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKS5sZWZ0IC0gZWxlbWVudC5zY3JvbGxMZWZ0ICsgd2luZG93LnBhZ2VYT2Zmc2V0ICsgY29vcmRpbmF0ZXMubGVmdH1weGA7XG4gIGNvbnNvbGUubG9nKHJlY3Quc3R5bGUudG9wKTtcbiAgY29uc29sZS5sb2cocmVjdC5zdHlsZS5sZWZ0KTtcbn1cbiJdfQ==