/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Platform } from '@angular/cdk/platform';
import { Directive, ElementRef, Input, NgZone } from '@angular/core';
import { fromEvent, Subject } from 'rxjs';
import { auditTime, takeUntil } from 'rxjs/operators';
/**
 * @record
 */
export function AutoSizeType() { }
if (false) {
    /** @type {?|undefined} */
    AutoSizeType.prototype.minRows;
    /** @type {?|undefined} */
    AutoSizeType.prototype.maxRows;
}
/**
 * @param {?} value
 * @return {?}
 */
export function isAutoSizeType(value) {
    return typeof value !== 'string' && typeof value !== 'boolean' && (!!value.maxRows || !!value.minRows);
}
export class NzAutosizeDirective {
    /**
     * @param {?} elementRef
     * @param {?} ngZone
     * @param {?} platform
     */
    constructor(elementRef, ngZone, platform) {
        this.elementRef = elementRef;
        this.ngZone = ngZone;
        this.platform = platform;
        this.autosize = false;
        this.el = this.elementRef.nativeElement;
        this.destroy$ = new Subject();
        this.inputGap = 10;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzAutosize(value) {
        if (typeof value === 'string') {
            this.autosize = true;
        }
        else if (isAutoSizeType(value)) {
            this.autosize = value;
            this.minRows = value.minRows;
            this.maxRows = value.maxRows;
            this.setMaxHeight();
            this.setMinHeight();
        }
    }
    /**
     * @return {?}
     */
    get nzAutosize() {
        return this.autosize;
    }
    /**
     * @param {?=} force
     * @return {?}
     */
    resizeToFitContent(force = false) {
        this.cacheTextareaLineHeight();
        // If we haven't determined the line-height yet, we know we're still hidden and there's no point
        // in checking the height of the textarea.
        if (!this.cachedLineHeight) {
            return;
        }
        /** @type {?} */
        const textarea = (/** @type {?} */ (this.el));
        /** @type {?} */
        const value = textarea.value;
        // Only resize if the value or minRows have changed since these calculations can be expensive.
        if (!force && this.minRows === this.previousMinRows && value === this.previousValue) {
            return;
        }
        /** @type {?} */
        const placeholderText = textarea.placeholder;
        // Reset the textarea height to auto in order to shrink back to its default size.
        // Also temporarily force overflow:hidden, so scroll bars do not interfere with calculations.
        // Long placeholders that are wider than the textarea width may lead to a bigger scrollHeight
        // value. To ensure that the scrollHeight is not bigger than the content, the placeholders
        // need to be removed temporarily.
        textarea.classList.add('cdk-textarea-autosize-measuring');
        textarea.placeholder = '';
        /** @type {?} */
        const height = Math.round((textarea.scrollHeight - this.inputGap) / this.cachedLineHeight) * this.cachedLineHeight +
            this.inputGap;
        // Use the scrollHeight to know how large the textarea *would* be if fit its entire value.
        textarea.style.height = `${height}px`;
        textarea.classList.remove('cdk-textarea-autosize-measuring');
        textarea.placeholder = placeholderText;
        // On Firefox resizing the textarea will prevent it from scrolling to the caret position.
        // We need to re-set the selection in order for it to scroll to the proper position.
        if (typeof requestAnimationFrame !== 'undefined') {
            this.ngZone.runOutsideAngular((/**
             * @return {?}
             */
            () => requestAnimationFrame((/**
             * @return {?}
             */
            () => {
                const { selectionStart, selectionEnd } = textarea;
                // IE will throw an "Unspecified error" if we try to set the selection range after the
                // element has been removed from the DOM. Assert that the directive hasn't been destroyed
                // between the time we requested the animation frame and when it was executed.
                // Also note that we have to assert that the textarea is focused before we set the
                // selection range. Setting the selection range on a non-focused textarea will cause
                // it to receive focus on IE and Edge.
                if (!this.destroy$.isStopped && document.activeElement === textarea) {
                    textarea.setSelectionRange(selectionStart, selectionEnd);
                }
            }))));
        }
        this.previousValue = value;
        this.previousMinRows = this.minRows;
    }
    /**
     * @private
     * @return {?}
     */
    cacheTextareaLineHeight() {
        if (this.cachedLineHeight >= 0 || !this.el.parentNode) {
            return;
        }
        // Use a clone element because we have to override some styles.
        /** @type {?} */
        const textareaClone = (/** @type {?} */ (this.el.cloneNode(false)));
        textareaClone.rows = 1;
        // Use `position: absolute` so that this doesn't cause a browser layout and use
        // `visibility: hidden` so that nothing is rendered. Clear any other styles that
        // would affect the height.
        textareaClone.style.position = 'absolute';
        textareaClone.style.visibility = 'hidden';
        textareaClone.style.border = 'none';
        textareaClone.style.padding = '0';
        textareaClone.style.height = '';
        textareaClone.style.minHeight = '';
        textareaClone.style.maxHeight = '';
        // In Firefox it happens that textarea elements are always bigger than the specified amount
        // of rows. This is because Firefox tries to add extra space for the horizontal scrollbar.
        // As a workaround that removes the extra space for the scrollbar, we can just set overflow
        // to hidden. This ensures that there is no invalid calculation of the line height.
        // See Firefox bug report: https://bugzilla.mozilla.org/show_bug.cgi?id=33654
        textareaClone.style.overflow = 'hidden';
        (/** @type {?} */ (this.el.parentNode)).appendChild(textareaClone);
        this.cachedLineHeight = textareaClone.clientHeight - this.inputGap - 1;
        (/** @type {?} */ (this.el.parentNode)).removeChild(textareaClone);
        // Min and max heights have to be re-calculated if the cached line height changes
        this.setMinHeight();
        this.setMaxHeight();
    }
    /**
     * @return {?}
     */
    setMinHeight() {
        /** @type {?} */
        const minHeight = this.minRows && this.cachedLineHeight ? `${this.minRows * this.cachedLineHeight + this.inputGap}px` : null;
        if (minHeight) {
            this.el.style.minHeight = minHeight;
        }
    }
    /**
     * @return {?}
     */
    setMaxHeight() {
        /** @type {?} */
        const maxHeight = this.maxRows && this.cachedLineHeight ? `${this.maxRows * this.cachedLineHeight + this.inputGap}px` : null;
        if (maxHeight) {
            this.el.style.maxHeight = maxHeight;
        }
    }
    /**
     * @return {?}
     */
    noopInputHandler() {
        // no-op handler that ensures we're running change detection on input events.
    }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        if (this.nzAutosize && this.platform.isBrowser) {
            this.resizeToFitContent();
            this.ngZone.runOutsideAngular((/**
             * @return {?}
             */
            () => {
                fromEvent(window, 'resize')
                    .pipe(auditTime(16), takeUntil(this.destroy$))
                    .subscribe((/**
                 * @return {?}
                 */
                () => this.resizeToFitContent(true)));
            }));
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
    /**
     * @return {?}
     */
    ngDoCheck() {
        if (this.nzAutosize && this.platform.isBrowser) {
            this.resizeToFitContent();
        }
    }
}
NzAutosizeDirective.decorators = [
    { type: Directive, args: [{
                selector: 'textarea[nzAutosize]',
                host: {
                    // Textarea elements that have the directive applied should have a single row by default.
                    // Browsers normally show two rows by default and therefore this limits the minRows binding.
                    rows: '1',
                    '(input)': 'noopInputHandler()'
                }
            },] }
];
/** @nocollapse */
NzAutosizeDirective.ctorParameters = () => [
    { type: ElementRef },
    { type: NgZone },
    { type: Platform }
];
NzAutosizeDirective.propDecorators = {
    nzAutosize: [{ type: Input }]
};
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.autosize;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.cachedLineHeight;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.previousValue;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.previousMinRows;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.minRows;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.maxRows;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.inputGap;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.ngZone;
    /**
     * @type {?}
     * @private
     */
    NzAutosizeDirective.prototype.platform;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYXV0b3NpemUuZGlyZWN0aXZlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImlucHV0L256LWF1dG9zaXplLmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBQ2pELE9BQU8sRUFBaUIsU0FBUyxFQUFXLFVBQVUsRUFBRSxLQUFLLEVBQUUsTUFBTSxFQUFhLE1BQU0sZUFBZSxDQUFDO0FBQ3hHLE9BQU8sRUFBRSxTQUFTLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQzFDLE9BQU8sRUFBRSxTQUFTLEVBQUUsU0FBUyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7Ozs7QUFFdEQsa0NBR0M7OztJQUZDLCtCQUFpQjs7SUFDakIsK0JBQWlCOzs7Ozs7QUFHbkIsTUFBTSxVQUFVLGNBQWMsQ0FBQyxLQUFzQztJQUNuRSxPQUFPLE9BQU8sS0FBSyxLQUFLLFFBQVEsSUFBSSxPQUFPLEtBQUssS0FBSyxTQUFTLElBQUksQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLE9BQU8sSUFBSSxDQUFDLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxDQUFDO0FBQ3pHLENBQUM7QUFXRCxNQUFNLE9BQU8sbUJBQW1COzs7Ozs7SUFnSjlCLFlBQW9CLFVBQXNCLEVBQVUsTUFBYyxFQUFVLFFBQWtCO1FBQTFFLGVBQVUsR0FBVixVQUFVLENBQVk7UUFBVSxXQUFNLEdBQU4sTUFBTSxDQUFRO1FBQVUsYUFBUSxHQUFSLFFBQVEsQ0FBVTtRQS9JdEYsYUFBUSxHQUEyQixLQUFLLENBQUM7UUFDekMsT0FBRSxHQUEyQyxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsQ0FBQztRQU0zRSxhQUFRLEdBQUcsSUFBSSxPQUFPLEVBQUUsQ0FBQztRQUN6QixhQUFRLEdBQUcsRUFBRSxDQUFDO0lBdUkyRSxDQUFDOzs7OztJQXJJbEcsSUFD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OztnQkFBQyxHQUFHLEVBQUUsQ0FBQyxJQUFJLENBQUMsa0JBQWtCLENBQUMsSUFBSSxDQUFDLEVBQUMsQ0FBQztZQUNwRCxDQUFDLEVBQUMsQ0FBQztTQUNKO0lBQ0gsQ0FBQzs7OztJQUVELFdBQVc7UUFDVCxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxDQUFDO1FBQ3JCLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDM0IsQ0FBQzs7OztJQUVELFNBQVM7UUFDUCxJQUFJLElBQUksQ0FBQyxVQUFVLElBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxTQUFTLEVBQUU7WUFDOUMsSUFBSSxDQUFDLGtCQUFrQixFQUFFLENBQUM7U0FDM0I7SUFDSCxDQUFDOzs7WUFsTEYsU0FBUyxTQUFDO2dCQUNULFFBQVEsRUFBRSxzQkFBc0I7Z0JBQ2hDLElBQUksRUFBRTs7O29CQUdKLElBQUksRUFBRSxHQUFHO29CQUNULFNBQVMsRUFBRSxvQkFBb0I7aUJBQ2hDO2FBQ0Y7Ozs7WUFyQjJDLFVBQVU7WUFBUyxNQUFNO1lBRDVELFFBQVE7Ozt5QkFrQ2QsS0FBSzs7Ozs7OztJQVZOLHVDQUFpRDs7Ozs7SUFDakQsaUNBQW1GOzs7OztJQUNuRiwrQ0FBaUM7Ozs7O0lBQ2pDLDRDQUE4Qjs7Ozs7SUFDOUIsOENBQTRDOzs7OztJQUM1QyxzQ0FBb0M7Ozs7O0lBQ3BDLHNDQUFvQzs7Ozs7SUFDcEMsdUNBQWlDOzs7OztJQUNqQyx1Q0FBc0I7Ozs7O0lBdUlWLHlDQUE4Qjs7Ozs7SUFBRSxxQ0FBc0I7Ozs7O0lBQUUsdUNBQTBCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgUGxhdGZvcm0gfSBmcm9tICdAYW5ndWxhci9jZGsvcGxhdGZvcm0nO1xuaW1wb3J0IHsgQWZ0ZXJWaWV3SW5pdCwgRGlyZWN0aXZlLCBEb0NoZWNrLCBFbGVtZW50UmVmLCBJbnB1dCwgTmdab25lLCBPbkRlc3Ryb3kgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IGZyb21FdmVudCwgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgYXVkaXRUaW1lLCB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgQXV0b1NpemVUeXBlIHtcbiAgbWluUm93cz86IG51bWJlcjtcbiAgbWF4Um93cz86IG51bWJlcjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQXV0b1NpemVUeXBlKHZhbHVlOiBzdHJpbmcgfCBib29sZWFuIHwgQXV0b1NpemVUeXBlKTogdmFsdWUgaXMgQXV0b1NpemVUeXBlIHtcbiAgcmV0dXJuIHR5cGVvZiB2YWx1ZSAhPT0gJ3N0cmluZycgJiYgdHlwZW9mIHZhbHVlICE9PSAnYm9vbGVhbicgJiYgKCEhdmFsdWUubWF4Um93cyB8fCAhIXZhbHVlLm1pblJvd3MpO1xufVxuXG5ARGlyZWN0aXZlKHtcbiAgc2VsZWN0b3I6ICd0ZXh0YXJlYVtuekF1dG9zaXplXScsXG4gIGhvc3Q6IHtcbiAgICAvLyBUZXh0YXJlYSBlbGVtZW50cyB0aGF0IGhhdmUgdGhlIGRpcmVjdGl2ZSBhcHBsaWVkIHNob3VsZCBoYXZlIGEgc2luZ2xlIHJvdyBieSBkZWZhdWx0LlxuICAgIC8vIEJyb3dzZXJzIG5vcm1hbGx5IHNob3cgdHdvIHJvd3MgYnkgZGVmYXVsdCBhbmQgdGhlcmVmb3JlIHRoaXMgbGltaXRzIHRoZSBtaW5Sb3dzIGJpbmRpbmcuXG4gICAgcm93czogJzEnLFxuICAgICcoaW5wdXQpJzogJ25vb3BJbnB1dEhhbmRsZXIoKSdcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOekF1dG9zaXplRGlyZWN0aXZlIGltcGxlbWVudHMgQWZ0ZXJWaWV3SW5pdCwgT25EZXN0cm95LCBEb0NoZWNrIHtcbiAgcHJpdmF0ZSBhdXRvc2l6ZTogYm9vbGVhbiB8IEF1dG9TaXplVHlwZSA9IGZhbHNlO1xuICBwcml2YXRlIGVsOiBIVE1MVGV4dEFyZWFFbGVtZW50IHwgSFRNTElucHV0RWxlbWVudCA9IHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50O1xuICBwcml2YXRlIGNhY2hlZExpbmVIZWlnaHQ6IG51bWJlcjtcbiAgcHJpdmF0ZSBwcmV2aW91c1ZhbHVlOiBzdHJpbmc7XG4gIHByaXZhdGUgcHJldmlvdXNNaW5Sb3dzOiBudW1iZXIgfCB1bmRlZmluZWQ7XG4gIHByaXZhdGUgbWluUm93czogbnVtYmVyIHwgdW5kZWZpbmVkO1xuICBwcml2YXRlIG1heFJvd3M6IG51bWJlciB8IHVuZGVmaW5lZDtcbiAgcHJpdmF0ZSBkZXN0cm95JCA9IG5ldyBTdWJqZWN0KCk7XG4gIHByaXZhdGUgaW5wdXRHYXAgPSAxMDtcblxuICBASW5wdXQoKVxuICBzZXQgbnpBdXRvc2l6ZSh2YWx1ZTogc3RyaW5nIHwgYm9vbGVhbiB8IEF1dG9TaXplVHlwZSkge1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09ICdzdHJpbmcnKSB7XG4gICAgICB0aGlzLmF1dG9zaXplID0gdHJ1ZTtcbiAgICB9IGVsc2UgaWYgKGlzQXV0b1NpemVUeXBlKHZhbHVlKSkge1xuICAgICAgdGhpcy5hdXRvc2l6ZSA9IHZhbHVlO1xuICAgICAgdGhpcy5taW5Sb3dzID0gdmFsdWUubWluUm93cztcbiAgICAgIHRoaXMubWF4Um93cyA9IHZhbHVlLm1heFJvd3M7XG4gICAgICB0aGlzLnNldE1heEhlaWdodCgpO1xuICAgICAgdGhpcy5zZXRNaW5IZWlnaHQoKTtcbiAgICB9XG4gIH1cblxuICBnZXQgbnpBdXRvc2l6ZSgpOiBzdHJpbmcgfCBib29sZWFuIHwgQXV0b1NpemVUeXBlIHtcbiAgICByZXR1cm4gdGhpcy5hdXRvc2l6ZTtcbiAgfVxuXG4gIHJlc2l6ZVRvRml0Q29udGVudChmb3JjZTogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgdGhpcy5jYWNoZVRleHRhcmVhTGluZUhlaWdodCgpO1xuXG4gICAgLy8gSWYgd2UgaGF2ZW4ndCBkZXRlcm1pbmVkIHRoZSBsaW5lLWhlaWdodCB5ZXQsIHdlIGtub3cgd2UncmUgc3RpbGwgaGlkZGVuIGFuZCB0aGVyZSdzIG5vIHBvaW50XG4gICAgLy8gaW4gY2hlY2tpbmcgdGhlIGhlaWdodCBvZiB0aGUgdGV4dGFyZWEuXG4gICAgaWYgKCF0aGlzLmNhY2hlZExpbmVIZWlnaHQpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCB0ZXh0YXJlYSA9IHRoaXMuZWwgYXMgSFRNTFRleHRBcmVhRWxlbWVudDtcbiAgICBjb25zdCB2YWx1ZSA9IHRleHRhcmVhLnZhbHVlO1xuXG4gICAgLy8gT25seSByZXNpemUgaWYgdGhlIHZhbHVlIG9yIG1pblJvd3MgaGF2ZSBjaGFuZ2VkIHNpbmNlIHRoZXNlIGNhbGN1bGF0aW9ucyBjYW4gYmUgZXhwZW5zaXZlLlxuICAgIGlmICghZm9yY2UgJiYgdGhpcy5taW5Sb3dzID09PSB0aGlzLnByZXZpb3VzTWluUm93cyAmJiB2YWx1ZSA9PT0gdGhpcy5wcmV2aW91c1ZhbHVlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGNvbnN0IHBsYWNlaG9sZGVyVGV4dCA9IHRleHRhcmVhLnBsYWNlaG9sZGVyO1xuXG4gICAgLy8gUmVzZXQgdGhlIHRleHRhcmVhIGhlaWdodCB0byBhdXRvIGluIG9yZGVyIHRvIHNocmluayBiYWNrIHRvIGl0cyBkZWZhdWx0IHNpemUuXG4gICAgLy8gQWxzbyB0ZW1wb3JhcmlseSBmb3JjZSBvdmVyZmxvdzpoaWRkZW4sIHNvIHNjcm9sbCBiYXJzIGRvIG5vdCBpbnRlcmZlcmUgd2l0aCBjYWxjdWxhdGlvbnMuXG4gICAgLy8gTG9uZyBwbGFjZWhvbGRlcnMgdGhhdCBhcmUgd2lkZXIgdGhhbiB0aGUgdGV4dGFyZWEgd2lkdGggbWF5IGxlYWQgdG8gYSBiaWdnZXIgc2Nyb2xsSGVpZ2h0XG4gICAgLy8gdmFsdWUuIFRvIGVuc3VyZSB0aGF0IHRoZSBzY3JvbGxIZWlnaHQgaXMgbm90IGJpZ2dlciB0aGFuIHRoZSBjb250ZW50LCB0aGUgcGxhY2Vob2xkZXJzXG4gICAgLy8gbmVlZCB0byBiZSByZW1vdmVkIHRlbXBvcmFyaWx5LlxuICAgIHRleHRhcmVhLmNsYXNzTGlzdC5hZGQoJ2Nkay10ZXh0YXJlYS1hdXRvc2l6ZS1tZWFzdXJpbmcnKTtcbiAgICB0ZXh0YXJlYS5wbGFjZWhvbGRlciA9ICcnO1xuICAgIGNvbnN0IGhlaWdodCA9XG4gICAgICBNYXRoLnJvdW5kKCh0ZXh0YXJlYS5zY3JvbGxIZWlnaHQgLSB0aGlzLmlucHV0R2FwKSAvIHRoaXMuY2FjaGVkTGluZUhlaWdodCkgKiB0aGlzLmNhY2hlZExpbmVIZWlnaHQgK1xuICAgICAgdGhpcy5pbnB1dEdhcDtcblxuICAgIC8vIFVzZSB0aGUgc2Nyb2xsSGVpZ2h0IHRvIGtub3cgaG93IGxhcmdlIHRoZSB0ZXh0YXJlYSAqd291bGQqIGJlIGlmIGZpdCBpdHMgZW50aXJlIHZhbHVlLlxuICAgIHRleHRhcmVhLnN0eWxlLmhlaWdodCA9IGAke2hlaWdodH1weGA7XG4gICAgdGV4dGFyZWEuY2xhc3NMaXN0LnJlbW92ZSgnY2RrLXRleHRhcmVhLWF1dG9zaXplLW1lYXN1cmluZycpO1xuICAgIHRleHRhcmVhLnBsYWNlaG9sZGVyID0gcGxhY2Vob2xkZXJUZXh0O1xuXG4gICAgLy8gT24gRmlyZWZveCByZXNpemluZyB0aGUgdGV4dGFyZWEgd2lsbCBwcmV2ZW50IGl0IGZyb20gc2Nyb2xsaW5nIHRvIHRoZSBjYXJldCBwb3NpdGlvbi5cbiAgICAvLyBXZSBuZWVkIHRvIHJlLXNldCB0aGUgc2VsZWN0aW9uIGluIG9yZGVyIGZvciBpdCB0byBzY3JvbGwgdG8gdGhlIHByb3BlciBwb3NpdGlvbi5cbiAgICBpZiAodHlwZW9mIHJlcXVlc3RBbmltYXRpb25GcmFtZSAhPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgIHRoaXMubmdab25lLnJ1bk91dHNpZGVBbmd1bGFyKCgpID0+XG4gICAgICAgIHJlcXVlc3RBbmltYXRpb25GcmFtZSgoKSA9PiB7XG4gICAgICAgICAgY29uc3QgeyBzZWxlY3Rpb25TdGFydCwgc2VsZWN0aW9uRW5kIH0gPSB0ZXh0YXJlYTtcblxuICAgICAgICAgIC8vIElFIHdpbGwgdGhyb3cgYW4gXCJVbnNwZWNpZmllZCBlcnJvclwiIGlmIHdlIHRyeSB0byBzZXQgdGhlIHNlbGVjdGlvbiByYW5nZSBhZnRlciB0aGVcbiAgICAgICAgICAvLyBlbGVtZW50IGhhcyBiZWVuIHJlbW92ZWQgZnJvbSB0aGUgRE9NLiBBc3NlcnQgdGhhdCB0aGUgZGlyZWN0aXZlIGhhc24ndCBiZWVuIGRlc3Ryb3llZFxuICAgICAgICAgIC8vIGJldHdlZW4gdGhlIHRpbWUgd2UgcmVxdWVzdGVkIHRoZSBhbmltYXRpb24gZnJhbWUgYW5kIHdoZW4gaXQgd2FzIGV4ZWN1dGVkLlxuICAgICAgICAgIC8vIEFsc28gbm90ZSB0aGF0IHdlIGhhdmUgdG8gYXNzZXJ0IHRoYXQgdGhlIHRleHRhcmVhIGlzIGZvY3VzZWQgYmVmb3JlIHdlIHNldCB0aGVcbiAgICAgICAgICAvLyBzZWxlY3Rpb24gcmFuZ2UuIFNldHRpbmcgdGhlIHNlbGVjdGlvbiByYW5nZSBvbiBhIG5vbi1mb2N1c2VkIHRleHRhcmVhIHdpbGwgY2F1c2VcbiAgICAgICAgICAvLyBpdCB0byByZWNlaXZlIGZvY3VzIG9uIElFIGFuZCBFZGdlLlxuICAgICAgICAgIGlmICghdGhpcy5kZXN0cm95JC5pc1N0b3BwZWQgJiYgZG9jdW1lbnQuYWN0aXZlRWxlbWVudCA9PT0gdGV4dGFyZWEpIHtcbiAgICAgICAgICAgIHRleHRhcmVhLnNldFNlbGVjdGlvblJhbmdlKHNlbGVjdGlvblN0YXJ0LCBzZWxlY3Rpb25FbmQpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSlcbiAgICAgICk7XG4gICAgfVxuXG4gICAgdGhpcy5wcmV2aW91c1ZhbHVlID0gdmFsdWU7XG4gICAgdGhpcy5wcmV2aW91c01pblJvd3MgPSB0aGlzLm1pblJvd3M7XG4gIH1cblxuICBwcml2YXRlIGNhY2hlVGV4dGFyZWFMaW5lSGVpZ2h0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmNhY2hlZExpbmVIZWlnaHQgPj0gMCB8fCAhdGhpcy5lbC5wYXJlbnROb2RlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gVXNlIGEgY2xvbmUgZWxlbWVudCBiZWNhdXNlIHdlIGhhdmUgdG8gb3ZlcnJpZGUgc29tZSBzdHlsZXMuXG4gICAgY29uc3QgdGV4dGFyZWFDbG9uZSA9IHRoaXMuZWwuY2xvbmVOb2RlKGZhbHNlKSBhcyBIVE1MVGV4dEFyZWFFbGVtZW50O1xuICAgIHRleHRhcmVhQ2xvbmUucm93cyA9IDE7XG5cbiAgICAvLyBVc2UgYHBvc2l0aW9uOiBhYnNvbHV0ZWAgc28gdGhhdCB0aGlzIGRvZXNuJ3QgY2F1c2UgYSBicm93c2VyIGxheW91dCBhbmQgdXNlXG4gICAgLy8gYHZpc2liaWxpdHk6IGhpZGRlbmAgc28gdGhhdCBub3RoaW5nIGlzIHJlbmRlcmVkLiBDbGVhciBhbnkgb3RoZXIgc3R5bGVzIHRoYXRcbiAgICAvLyB3b3VsZCBhZmZlY3QgdGhlIGhlaWdodC5cbiAgICB0ZXh0YXJlYUNsb25lLnN0eWxlLnBvc2l0aW9uID0gJ2Fic29sdXRlJztcbiAgICB0ZXh0YXJlYUNsb25lLnN0eWxlLnZpc2liaWxpdHkgPSAnaGlkZGVuJztcbiAgICB0ZXh0YXJlYUNsb25lLnN0eWxlLmJvcmRlciA9ICdub25lJztcbiAgICB0ZXh0YXJlYUNsb25lLnN0eWxlLnBhZGRpbmcgPSAnMCc7XG4gICAgdGV4dGFyZWFDbG9uZS5zdHlsZS5oZWlnaHQgPSAnJztcbiAgICB0ZXh0YXJlYUNsb25lLnN0eWxlLm1pbkhlaWdodCA9ICcnO1xuICAgIHRleHRhcmVhQ2xvbmUuc3R5bGUubWF4SGVpZ2h0ID0gJyc7XG5cbiAgICAvLyBJbiBGaXJlZm94IGl0IGhhcHBlbnMgdGhhdCB0ZXh0YXJlYSBlbGVtZW50cyBhcmUgYWx3YXlzIGJpZ2dlciB0aGFuIHRoZSBzcGVjaWZpZWQgYW1vdW50XG4gICAgLy8gb2Ygcm93cy4gVGhpcyBpcyBiZWNhdXNlIEZpcmVmb3ggdHJpZXMgdG8gYWRkIGV4dHJhIHNwYWNlIGZvciB0aGUgaG9yaXpvbnRhbCBzY3JvbGxiYXIuXG4gICAgLy8gQXMgYSB3b3JrYXJvdW5kIHRoYXQgcmVtb3ZlcyB0aGUgZXh0cmEgc3BhY2UgZm9yIHRoZSBzY3JvbGxiYXIsIHdlIGNhbiBqdXN0IHNldCBvdmVyZmxvd1xuICAgIC8vIHRvIGhpZGRlbi4gVGhpcyBlbnN1cmVzIHRoYXQgdGhlcmUgaXMgbm8gaW52YWxpZCBjYWxjdWxhdGlvbiBvZiB0aGUgbGluZSBoZWlnaHQuXG4gICAgLy8gU2VlIEZpcmVmb3ggYnVnIHJlcG9ydDogaHR0cHM6Ly9idWd6aWxsYS5tb3ppbGxhLm9yZy9zaG93X2J1Zy5jZ2k/aWQ9MzM2NTRcbiAgICB0ZXh0YXJlYUNsb25lLnN0eWxlLm92ZXJmbG93ID0gJ2hpZGRlbic7XG5cbiAgICB0aGlzLmVsLnBhcmVudE5vZGUhLmFwcGVuZENoaWxkKHRleHRhcmVhQ2xvbmUpO1xuICAgIHRoaXMuY2FjaGVkTGluZUhlaWdodCA9IHRleHRhcmVhQ2xvbmUuY2xpZW50SGVpZ2h0IC0gdGhpcy5pbnB1dEdhcCAtIDE7XG4gICAgdGhpcy5lbC5wYXJlbnROb2RlIS5yZW1vdmVDaGlsZCh0ZXh0YXJlYUNsb25lKTtcblxuICAgIC8vIE1pbiBhbmQgbWF4IGhlaWdodHMgaGF2ZSB0byBiZSByZS1jYWxjdWxhdGVkIGlmIHRoZSBjYWNoZWQgbGluZSBoZWlnaHQgY2hhbmdlc1xuICAgIHRoaXMuc2V0TWluSGVpZ2h0KCk7XG4gICAgdGhpcy5zZXRNYXhIZWlnaHQoKTtcbiAgfVxuXG4gIHNldE1pbkhlaWdodCgpOiB2b2lkIHtcbiAgICBjb25zdCBtaW5IZWlnaHQgPVxuICAgICAgdGhpcy5taW5Sb3dzICYmIHRoaXMuY2FjaGVkTGluZUhlaWdodCA/IGAke3RoaXMubWluUm93cyAqIHRoaXMuY2FjaGVkTGluZUhlaWdodCArIHRoaXMuaW5wdXRHYXB9cHhgIDogbnVsbDtcblxuICAgIGlmIChtaW5IZWlnaHQpIHtcbiAgICAgIHRoaXMuZWwuc3R5bGUubWluSGVpZ2h0ID0gbWluSGVpZ2h0O1xuICAgIH1cbiAgfVxuXG4gIHNldE1heEhlaWdodCgpOiB2b2lkIHtcbiAgICBjb25zdCBtYXhIZWlnaHQgPVxuICAgICAgdGhpcy5tYXhSb3dzICYmIHRoaXMuY2FjaGVkTGluZUhlaWdodCA/IGAke3RoaXMubWF4Um93cyAqIHRoaXMuY2FjaGVkTGluZUhlaWdodCArIHRoaXMuaW5wdXRHYXB9cHhgIDogbnVsbDtcblxuICAgIGlmIChtYXhIZWlnaHQpIHtcbiAgICAgIHRoaXMuZWwuc3R5bGUubWF4SGVpZ2h0ID0gbWF4SGVpZ2h0O1xuICAgIH1cbiAgfVxuXG4gIG5vb3BJbnB1dEhhbmRsZXIoKTogdm9pZCB7XG4gICAgLy8gbm8tb3AgaGFuZGxlciB0aGF0IGVuc3VyZXMgd2UncmUgcnVubmluZyBjaGFuZ2UgZGV0ZWN0aW9uIG9uIGlucHV0IGV2ZW50cy5cbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZiwgcHJpdmF0ZSBuZ1pvbmU6IE5nWm9uZSwgcHJpdmF0ZSBwbGF0Zm9ybTogUGxhdGZvcm0pIHt9XG5cbiAgbmdBZnRlclZpZXdJbml0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56QXV0b3NpemUgJiYgdGhpcy5wbGF0Zm9ybS5pc0Jyb3dzZXIpIHtcbiAgICAgIHRoaXMucmVzaXplVG9GaXRDb250ZW50KCk7XG4gICAgICB0aGlzLm5nWm9uZS5ydW5PdXRzaWRlQW5ndWxhcigoKSA9PiB7XG4gICAgICAgIGZyb21FdmVudCh3aW5kb3csICdyZXNpemUnKVxuICAgICAgICAgIC5waXBlKFxuICAgICAgICAgICAgYXVkaXRUaW1lKDE2KSxcbiAgICAgICAgICAgIHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKVxuICAgICAgICAgIClcbiAgICAgICAgICAuc3Vic2NyaWJlKCgpID0+IHRoaXMucmVzaXplVG9GaXRDb250ZW50KHRydWUpKTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZGVzdHJveSQubmV4dCgpO1xuICAgIHRoaXMuZGVzdHJveSQuY29tcGxldGUoKTtcbiAgfVxuXG4gIG5nRG9DaGVjaygpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekF1dG9zaXplICYmIHRoaXMucGxhdGZvcm0uaXNCcm93c2VyKSB7XG4gICAgICB0aGlzLnJlc2l6ZVRvRml0Q29udGVudCgpO1xuICAgIH1cbiAgfVxufVxuIl19