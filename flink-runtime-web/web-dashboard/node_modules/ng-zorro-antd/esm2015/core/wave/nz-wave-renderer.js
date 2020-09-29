/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Platform } from '@angular/cdk/platform';
export class NzWaveRenderer {
    /**
     * @param {?} triggerElement
     * @param {?} ngZone
     * @param {?} insertExtraNode
     */
    constructor(triggerElement, ngZone, insertExtraNode) {
        this.triggerElement = triggerElement;
        this.ngZone = ngZone;
        this.insertExtraNode = insertExtraNode;
        this.waveTransitionDuration = 400;
        this.lastTime = 0;
        this.onClick = (/**
         * @param {?} event
         * @return {?}
         */
        (event) => {
            if (!this.triggerElement ||
                !this.triggerElement.getAttribute ||
                this.triggerElement.getAttribute('disabled') ||
                ((/** @type {?} */ (event.target))).tagName === 'INPUT' ||
                this.triggerElement.className.indexOf('disabled') >= 0) {
                return;
            }
            this.fadeOutWave();
        });
        /** @type {?} */
        const platform = new Platform();
        if (platform.isBrowser) {
            this.bindTriggerEvent();
        }
    }
    /**
     * @return {?}
     */
    get waveAttributeName() {
        return this.insertExtraNode ? 'ant-click-animating' : 'ant-click-animating-without-extra-node';
    }
    /**
     * @return {?}
     */
    bindTriggerEvent() {
        this.ngZone.runOutsideAngular((/**
         * @return {?}
         */
        () => {
            if (this.triggerElement) {
                this.triggerElement.addEventListener('click', this.onClick, true);
            }
        }));
    }
    /**
     * @return {?}
     */
    removeTriggerEvent() {
        if (this.triggerElement) {
            this.triggerElement.removeEventListener('click', this.onClick, true);
        }
    }
    /**
     * @return {?}
     */
    removeStyleAndExtraNode() {
        if (this.styleForPseudo && document.body.contains(this.styleForPseudo)) {
            document.body.removeChild(this.styleForPseudo);
            this.styleForPseudo = null;
        }
        if (this.insertExtraNode && this.triggerElement.contains(this.extraNode)) {
            this.triggerElement.removeChild((/** @type {?} */ (this.extraNode)));
        }
    }
    /**
     * @return {?}
     */
    destroy() {
        this.removeTriggerEvent();
        this.removeStyleAndExtraNode();
    }
    /**
     * @private
     * @return {?}
     */
    fadeOutWave() {
        /** @type {?} */
        const node = this.triggerElement;
        /** @type {?} */
        const waveColor = this.getWaveColor(node);
        node.setAttribute(this.waveAttributeName, 'true');
        if (Date.now() < this.lastTime + this.waveTransitionDuration) {
            return;
        }
        if (this.isValidColor(waveColor)) {
            if (!this.styleForPseudo) {
                this.styleForPseudo = document.createElement('style');
            }
            this.styleForPseudo.innerHTML =
                `[ant-click-animating-without-extra-node]:after { border-color: ${waveColor}; }`;
            document.body.appendChild(this.styleForPseudo);
        }
        if (this.insertExtraNode) {
            if (!this.extraNode) {
                this.extraNode = document.createElement('div');
            }
            this.extraNode.className = 'ant-click-animating-node';
            node.appendChild(this.extraNode);
        }
        this.lastTime = Date.now();
        this.runTimeoutOutsideZone((/**
         * @return {?}
         */
        () => {
            node.removeAttribute(this.waveAttributeName);
            this.removeStyleAndExtraNode();
        }), this.waveTransitionDuration);
    }
    /**
     * @private
     * @param {?} color
     * @return {?}
     */
    isValidColor(color) {
        return !!color
            && color !== '#ffffff'
            && color !== 'rgb(255, 255, 255)'
            && this.isNotGrey(color)
            && !/rgba\(\d*, \d*, \d*, 0\)/.test(color)
            && color !== 'transparent';
    }
    /**
     * @private
     * @param {?} color
     * @return {?}
     */
    isNotGrey(color) {
        /** @type {?} */
        const match = color.match(/rgba?\((\d*), (\d*), (\d*)(, [\.\d]*)?\)/);
        if (match && match[1] && match[2] && match[3]) {
            return !(match[1] === match[2] && match[2] === match[3]);
        }
        return true;
    }
    /**
     * @private
     * @param {?} node
     * @return {?}
     */
    getWaveColor(node) {
        /** @type {?} */
        const nodeStyle = getComputedStyle(node);
        return nodeStyle.getPropertyValue('border-top-color') || // Firefox Compatible
            nodeStyle.getPropertyValue('border-color') ||
            nodeStyle.getPropertyValue('background-color');
    }
    /**
     * @private
     * @param {?} fn
     * @param {?} delay
     * @return {?}
     */
    runTimeoutOutsideZone(fn, delay) {
        this.ngZone.runOutsideAngular((/**
         * @return {?}
         */
        () => setTimeout(fn, delay)));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzWaveRenderer.prototype.waveTransitionDuration;
    /**
     * @type {?}
     * @private
     */
    NzWaveRenderer.prototype.styleForPseudo;
    /**
     * @type {?}
     * @private
     */
    NzWaveRenderer.prototype.extraNode;
    /**
     * @type {?}
     * @private
     */
    NzWaveRenderer.prototype.lastTime;
    /** @type {?} */
    NzWaveRenderer.prototype.onClick;
    /**
     * @type {?}
     * @private
     */
    NzWaveRenderer.prototype.triggerElement;
    /**
     * @type {?}
     * @private
     */
    NzWaveRenderer.prototype.ngZone;
    /**
     * @type {?}
     * @private
     */
    NzWaveRenderer.prototype.insertExtraNode;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotd2F2ZS1yZW5kZXJlci5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL3dhdmUvbnotd2F2ZS1yZW5kZXJlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBR2pELE1BQU0sT0FBTyxjQUFjOzs7Ozs7SUFXekIsWUFBb0IsY0FBMkIsRUFBVSxNQUFjLEVBQVUsZUFBd0I7UUFBckYsbUJBQWMsR0FBZCxjQUFjLENBQWE7UUFBVSxXQUFNLEdBQU4sTUFBTSxDQUFRO1FBQVUsb0JBQWUsR0FBZixlQUFlLENBQVM7UUFUakcsMkJBQXNCLEdBQUcsR0FBRyxDQUFDO1FBRzdCLGFBQVEsR0FBRyxDQUFDLENBQUM7UUFhckIsWUFBTzs7OztRQUFHLENBQUMsS0FBaUIsRUFBRSxFQUFFO1lBQzlCLElBQ0UsQ0FBQyxJQUFJLENBQUMsY0FBYztnQkFDcEIsQ0FBQyxJQUFJLENBQUMsY0FBYyxDQUFDLFlBQVk7Z0JBQ2pDLElBQUksQ0FBQyxjQUFjLENBQUMsWUFBWSxDQUFDLFVBQVUsQ0FBQztnQkFDNUMsQ0FBQyxtQkFBQSxLQUFLLENBQUMsTUFBTSxFQUFlLENBQUMsQ0FBQyxPQUFPLEtBQUssT0FBTztnQkFDakQsSUFBSSxDQUFDLGNBQWMsQ0FBQyxTQUFTLENBQUMsT0FBTyxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsRUFBRTtnQkFDeEQsT0FBTzthQUNSO1lBQ0QsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO1FBQ3JCLENBQUMsRUFBQTs7Y0FoQk8sUUFBUSxHQUFHLElBQUksUUFBUSxFQUFFO1FBQy9CLElBQUksUUFBUSxDQUFDLFNBQVMsRUFBRTtZQUN0QixJQUFJLENBQUMsZ0JBQWdCLEVBQUUsQ0FBQztTQUN6QjtJQUNILENBQUM7Ozs7SUFURCxJQUFJLGlCQUFpQjtRQUNuQixPQUFPLElBQUksQ0FBQyxlQUFlLENB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QkFBaUI7OztRQUFDLEdBQUcsRUFBRSxDQUFDLFVBQVUsQ0FBQyxFQUFFLEVBQUUsS0FBSyxDQUFDLEVBQUMsQ0FBQztJQUM3RCxDQUFDO0NBQ0Y7Ozs7OztJQXRIQyxnREFBcUM7Ozs7O0lBQ3JDLHdDQUFnRDs7Ozs7SUFDaEQsbUNBQXlDOzs7OztJQUN6QyxrQ0FBcUI7O0lBYXJCLGlDQVVDOzs7OztJQWpCVyx3Q0FBbUM7Ozs7O0lBQUUsZ0NBQXNCOzs7OztJQUFFLHlDQUFnQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IFBsYXRmb3JtIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL3BsYXRmb3JtJztcbmltcG9ydCB7IE5nWm9uZSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5leHBvcnQgY2xhc3MgTnpXYXZlUmVuZGVyZXIge1xuXG4gIHByaXZhdGUgd2F2ZVRyYW5zaXRpb25EdXJhdGlvbiA9IDQwMDtcbiAgcHJpdmF0ZSBzdHlsZUZvclBzZXVkbzogSFRNTFN0eWxlRWxlbWVudCB8IG51bGw7XG4gIHByaXZhdGUgZXh0cmFOb2RlOiBIVE1MRGl2RWxlbWVudCB8IG51bGw7XG4gIHByaXZhdGUgbGFzdFRpbWUgPSAwO1xuXG4gIGdldCB3YXZlQXR0cmlidXRlTmFtZSgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLmluc2VydEV4dHJhTm9kZSA/ICdhbnQtY2xpY2stYW5pbWF0aW5nJyA6ICdhbnQtY2xpY2stYW5pbWF0aW5nLXdpdGhvdXQtZXh0cmEtbm9kZSc7XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHRyaWdnZXJFbGVtZW50OiBIVE1MRWxlbWVudCwgcHJpdmF0ZSBuZ1pvbmU6IE5nWm9uZSwgcHJpdmF0ZSBpbnNlcnRFeHRyYU5vZGU6IGJvb2xlYW4pIHtcbiAgICBjb25zdCBwbGF0Zm9ybSA9IG5ldyBQbGF0Zm9ybSgpO1xuICAgIGlmIChwbGF0Zm9ybS5pc0Jyb3dzZXIpIHtcbiAgICAgIHRoaXMuYmluZFRyaWdnZXJFdmVudCgpO1xuICAgIH1cbiAgfVxuXG4gIG9uQ2xpY2sgPSAoZXZlbnQ6IE1vdXNlRXZlbnQpID0+IHtcbiAgICBpZiAoXG4gICAgICAhdGhpcy50cmlnZ2VyRWxlbWVudCB8fFxuICAgICAgIXRoaXMudHJpZ2dlckVsZW1lbnQuZ2V0QXR0cmlidXRlIHx8XG4gICAgICB0aGlzLnRyaWdnZXJFbGVtZW50LmdldEF0dHJpYnV0ZSgnZGlzYWJsZWQnKSB8fFxuICAgICAgKGV2ZW50LnRhcmdldCBhcyBIVE1MRWxlbWVudCkudGFnTmFtZSA9PT0gJ0lOUFVUJyB8fFxuICAgICAgdGhpcy50cmlnZ2VyRWxlbWVudC5jbGFzc05hbWUuaW5kZXhPZignZGlzYWJsZWQnKSA+PSAwKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuZmFkZU91dFdhdmUoKTtcbiAgfVxuXG4gIGJpbmRUcmlnZ2VyRXZlbnQoKTogdm9pZCB7XG4gICAgdGhpcy5uZ1pvbmUucnVuT3V0c2lkZUFuZ3VsYXIoKCkgPT4ge1xuICAgICAgaWYgKHRoaXMudHJpZ2dlckVsZW1lbnQpIHtcbiAgICAgICAgdGhpcy50cmlnZ2VyRWxlbWVudC5hZGRFdmVudExpc3RlbmVyKCdjbGljaycsIHRoaXMub25DbGljaywgdHJ1ZSk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICByZW1vdmVUcmlnZ2VyRXZlbnQoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMudHJpZ2dlckVsZW1lbnQpIHtcbiAgICAgIHRoaXMudHJpZ2dlckVsZW1lbnQucmVtb3ZlRXZlbnRMaXN0ZW5lcignY2xpY2snLCB0aGlzLm9uQ2xpY2ssIHRydWUpO1xuICAgIH1cbiAgfVxuXG4gIHJlbW92ZVN0eWxlQW5kRXh0cmFOb2RlKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnN0eWxlRm9yUHNldWRvICYmIGRvY3VtZW50LmJvZHkuY29udGFpbnModGhpcy5zdHlsZUZvclBzZXVkbykpIHtcbiAgICAgIGRvY3VtZW50LmJvZHkucmVtb3ZlQ2hpbGQodGhpcy5zdHlsZUZvclBzZXVkbyk7XG4gICAgICB0aGlzLnN0eWxlRm9yUHNldWRvID0gbnVsbDtcbiAgICB9XG4gICAgaWYgKHRoaXMuaW5zZXJ0RXh0cmFOb2RlICYmIHRoaXMudHJpZ2dlckVsZW1lbnQuY29udGFpbnModGhpcy5leHRyYU5vZGUpKSB7XG4gICAgICB0aGlzLnRyaWdnZXJFbGVtZW50LnJlbW92ZUNoaWxkKHRoaXMuZXh0cmFOb2RlIGFzIE5vZGUpO1xuICAgIH1cbiAgfVxuXG4gIGRlc3Ryb3koKTogdm9pZCB7XG4gICB0aGlzLnJlbW92ZVRyaWdnZXJFdmVudCgpO1xuICAgdGhpcy5yZW1vdmVTdHlsZUFuZEV4dHJhTm9kZSgpO1xuICB9XG5cbiAgcHJpdmF0ZSBmYWRlT3V0V2F2ZSgpOiB2b2lkIHtcbiAgICBjb25zdCBub2RlID0gdGhpcy50cmlnZ2VyRWxlbWVudDtcbiAgICBjb25zdCB3YXZlQ29sb3IgPSB0aGlzLmdldFdhdmVDb2xvcihub2RlKTtcbiAgICBub2RlLnNldEF0dHJpYnV0ZSh0aGlzLndhdmVBdHRyaWJ1dGVOYW1lLCAndHJ1ZScpO1xuICAgIGlmIChEYXRlLm5vdygpIDwgdGhpcy5sYXN0VGltZSArIHRoaXMud2F2ZVRyYW5zaXRpb25EdXJhdGlvbikge1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGlmICh0aGlzLmlzVmFsaWRDb2xvcih3YXZlQ29sb3IpKSB7XG4gICAgICBpZiAoIXRoaXMuc3R5bGVGb3JQc2V1ZG8pIHtcbiAgICAgICAgdGhpcy5zdHlsZUZvclBzZXVkbyA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3N0eWxlJyk7XG4gICAgICB9XG5cbiAgICAgIHRoaXMuc3R5bGVGb3JQc2V1ZG8uaW5uZXJIVE1MID1cbiAgICAgICAgYFthbnQtY2xpY2stYW5pbWF0aW5nLXdpdGhvdXQtZXh0cmEtbm9kZV06YWZ0ZXIgeyBib3JkZXItY29sb3I6ICR7d2F2ZUNvbG9yfTsgfWA7XG4gICAgICBkb2N1bWVudC5ib2R5LmFwcGVuZENoaWxkKHRoaXMuc3R5bGVGb3JQc2V1ZG8pO1xuICAgIH1cblxuICAgIGlmICh0aGlzLmluc2VydEV4dHJhTm9kZSkge1xuICAgICAgaWYgKCF0aGlzLmV4dHJhTm9kZSkge1xuICAgICAgICB0aGlzLmV4dHJhTm9kZSA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2RpdicpO1xuICAgICAgfVxuICAgICAgdGhpcy5leHRyYU5vZGUuY2xhc3NOYW1lID0gJ2FudC1jbGljay1hbmltYXRpbmctbm9kZSc7XG4gICAgICBub2RlLmFwcGVuZENoaWxkKHRoaXMuZXh0cmFOb2RlKTtcbiAgICB9XG5cbiAgICB0aGlzLmxhc3RUaW1lID0gRGF0ZS5ub3coKTtcblxuICAgIHRoaXMucnVuVGltZW91dE91dHNpZGVab25lKCgpID0+IHtcbiAgICAgIG5vZGUucmVtb3ZlQXR0cmlidXRlKHRoaXMud2F2ZUF0dHJpYnV0ZU5hbWUpO1xuICAgICAgdGhpcy5yZW1vdmVTdHlsZUFuZEV4dHJhTm9kZSgpO1xuICAgIH0sIHRoaXMud2F2ZVRyYW5zaXRpb25EdXJhdGlvbik7XG4gIH1cblxuICBwcml2YXRlIGlzVmFsaWRDb2xvcihjb2xvcjogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuICEhY29sb3JcbiAgICAgICYmIGNvbG9yICE9PSAnI2ZmZmZmZidcbiAgICAgICYmIGNvbG9yICE9PSAncmdiKDI1NSwgMjU1LCAyNTUpJ1xuICAgICAgJiYgdGhpcy5pc05vdEdyZXkoY29sb3IpXG4gICAgICAmJiAhL3JnYmFcXChcXGQqLCBcXGQqLCBcXGQqLCAwXFwpLy50ZXN0KGNvbG9yKVxuICAgICAgJiYgY29sb3IgIT09ICd0cmFuc3BhcmVudCc7XG4gIH1cblxuICBwcml2YXRlIGlzTm90R3JleShjb2xvcjogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgY29uc3QgbWF0Y2ggPSBjb2xvci5tYXRjaCgvcmdiYT9cXCgoXFxkKiksIChcXGQqKSwgKFxcZCopKCwgW1xcLlxcZF0qKT9cXCkvKTtcbiAgICBpZiAobWF0Y2ggJiYgbWF0Y2hbIDEgXSAmJiBtYXRjaFsgMiBdICYmIG1hdGNoWyAzIF0pIHtcbiAgICAgIHJldHVybiAhKG1hdGNoWyAxIF0gPT09IG1hdGNoWyAyIF0gJiYgbWF0Y2hbIDIgXSA9PT0gbWF0Y2hbIDMgXSk7XG4gICAgfVxuICAgIHJldHVybiB0cnVlO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRXYXZlQ29sb3Iobm9kZTogSFRNTEVsZW1lbnQpOiBzdHJpbmcge1xuICAgIGNvbnN0IG5vZGVTdHlsZSA9IGdldENvbXB1dGVkU3R5bGUobm9kZSk7XG4gICAgcmV0dXJuIG5vZGVTdHlsZS5nZXRQcm9wZXJ0eVZhbHVlKCdib3JkZXItdG9wLWNvbG9yJykgfHwgLy8gRmlyZWZveCBDb21wYXRpYmxlXG4gICAgICBub2RlU3R5bGUuZ2V0UHJvcGVydHlWYWx1ZSgnYm9yZGVyLWNvbG9yJykgfHxcbiAgICAgIG5vZGVTdHlsZS5nZXRQcm9wZXJ0eVZhbHVlKCdiYWNrZ3JvdW5kLWNvbG9yJyk7XG4gIH1cblxuICBwcml2YXRlIHJ1blRpbWVvdXRPdXRzaWRlWm9uZShmbjogKCkgPT4gdm9pZCwgZGVsYXk6IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMubmdab25lLnJ1bk91dHNpZGVBbmd1bGFyKCgpID0+IHNldFRpbWVvdXQoZm4sIGRlbGF5KSk7XG4gIH1cbn1cbiJdfQ==